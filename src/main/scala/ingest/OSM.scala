package ingest

import scala.util.{Try, Failure, Success}

import cats.data.Reader
import cats.implicits._
import com.monovore.decline._
import com.monovore.decline.refined._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import geotrellis.proj4.LatLng
import geotrellis.raster._
import geotrellis.raster.rasterize.CellValue
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.s3._
import geotrellis.spark.tiling._
import geotrellis.spark.io.kryo._
import geotrellis.vector._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.lit
import vectorpipe._

// --- //

case class OSMEnv(
  ss:         SparkSession,
  partitions: Int,
  writer:     S3LayerWriter,
  layer:      LayerId,
  layout:     LayoutDefinition
)

object OSM extends CommandApp(

  name   = "osm-ingest",
  header = "Perform the OSM-only Ingest",
  main   = {

    /* Ensures that only positive, non-zero values can be given as arguments. */
    type UInt = Int Refined Positive

    val partO: Opts[UInt]   = Opts.option[UInt]("partitions", help = "Spark partitions to use.").withDefault(5000)
    val pathO: Opts[String] = Opts.option[String]("orc", help = "Path to an ORC file to rasterize.")
    val outpO: Opts[String] = Opts.option[String]("layer", help = "Name of the output layer.")

    (partO, pathO, outpO).mapN { (numPartitions, orc, outputLayer) =>

      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("Ingest OSM")
        .set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)

      implicit val ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate

      val env = OSMEnv(
        ss,
        numPartitions,
        S3LayerWriter(S3AttributeStore("geotrellis-test", "dg-srtm")),
        LayerId(outputLayer, 13),
        ZoomedLayoutScheme.layoutForZoom(13, LatLng.worldExtent, 256)
      )

      Try(osm.fromDataFrame(OSMWork.patchData(ss.read.orc(orc)))) match {
        case Failure(e) => ss.stop(); throw e
        case Success((ns, ws, _)) => (OSMWork.rasterize(ns, ws) >>= OSMWork.write _).run(env)
      }
    }
  }
)

object OSMWork {

  type Layer = RDD[(SpatialKey, Tile)]

  def patchData(df: DataFrame): DataFrame = df.withColumn("visible", lit(true))

  def rasterize(nodes: RDD[(Long, osm.Node)], ways: RDD[(Long, osm.Way)]): Reader[OSMEnv, Layer] = Reader { env =>

    val roadsAndWater: RDD[(Long, osm.Way)] = ways.repartition(env.partitions / 2).filter { case (_, w) =>
      w.meta.tags.contains("highway") || w.meta.tags.contains("waterway")
    }

    val features: osm.Features = osm.snapshotFeatures(
      VectorPipe.logNothing, nodes.repartition(env.partitions / 2), roadsAndWater, env.ss.sparkContext.emptyRDD
    )

    val fused: RDD[Feature[Geometry, osm.ElementMeta]] =
      env.ss.sparkContext.union(features.lines.map(identity), features.polygons.map(identity))

    /* The `CellValue`s given here will place roads above water in the rasterized
     * Tile, with the assumption that some bridge went overtop.
     */
    val lines: RDD[Feature[Geometry, CellValue]] = fused.map {
      case Feature(g, m) if m.tags.contains("highway") => Feature(g, CellValue(2, 2))
      case Feature(g, _) => Feature(g, CellValue(1, 1))  /* Assume it's a waterway */
    }

    lines.rasterize(ByteCellType, env.layout)
  }

  def write(layer: Layer): Reader[OSMEnv, Unit] = Reader { env =>
    val persisted: Layer = layer.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val bounds: KeyBounds[SpatialKey] =
      persisted.map { case (key, _) => KeyBounds(key, key) }.reduce(_ combine _)

    val meta = vectorpipe.util.LayerMetadata(env.layout, bounds)

    env.writer.write(env.layer, ContextRDD(persisted, meta), ZCurveKeyIndexMethod)
  }
}
