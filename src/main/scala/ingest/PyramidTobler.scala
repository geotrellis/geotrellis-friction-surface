package ingest

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.rasterize.CellValue
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.kryo._
import geotrellis.spark.io.s3._
import geotrellis.spark.tiling._
import geotrellis.vector._

import cats.implicits._
import com.monovore.decline._
import com.monovore.decline.refined._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer
import vectorpipe._

import scala.util.{ Try, Success, Failure }

// --- //

case class Env(
  layerName: String,
  resultName: String,
  layoutScheme: ZoomedLayoutScheme,
  numPartitions: Int,
  orcPath: String,
  tiny: Boolean,
  sparkConf: SparkConf,
  reader: LayerReader[LayerId],
  writer: LayerWriter[LayerId]
) {
  val partitioner = new ZPartitioner(numPartitions)
}

object ToblerPyramid extends CommandApp(

  name   = "tobler-ingest",
  header = "Perform the Tobler Ingest",
  main   = {

    /* Ensures that only positive, non-zero values can be given as arguments. */
    type UInt = Int Refined Positive

    val partO: Opts[UInt]    = Opts.option[UInt]("partitions", help = "Spark partitions to use.").withDefault(15000)
    val pathO: Opts[String]  = Opts.option[String]("orc",      help = "Path to an ORC file to overlay.")
    val outpO: Opts[String]  = Opts.option[String]("layer",    help = "Name of the output layer.")
    val tinyF: Opts[Boolean] = Opts.flag("tiny", help = "Constrain the job to only run on California.").orFalse

    (partO, pathO, outpO, tinyF).mapN { (numPartitions, orc, outputLayer, tiny) =>

      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("Tobler Ingest")
        .set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)

      implicit val ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate

      val bucket = "geotrellis-test"
      val prefix = "dg-srtm"

      val env: Env = Env(
        layerName     = "srtm-wsg84-gps",
        resultName    = outputLayer,
        layoutScheme  = ZoomedLayoutScheme(WebMercator),
        numPartitions = numPartitions.value,
        orcPath       = orc,
        tiny          = tiny,
        sparkConf     = conf,
        reader        = S3LayerReader(bucket, prefix)(ss.sparkContext),
        writer        = S3LayerWriter(bucket, prefix)
      )

      (Work.roadOverlay(env) >>= { FullPyramid(env, _) }) match {
        case Failure(e) => ss.stop; throw e
        case Success(_) => ss.stop
      }
    }
  }
)

object Work {

  /** Fetch the layer with overlain Geometries, or perform said operation if it
    * hasn't occured yet.
    */
  def roadOverlay(env: Env)(implicit ss: SparkSession): Try[TileLayerRDD[SpatialKey]] = {
    sourceLayer(env).map(tobler) >>= { osmLayer(env, _) }
  }

  /** Reduced in size if this is a local ingest. */
  def sourceLayer(env: Env): Try[MultibandTileLayerRDD[SpatialKey]] = Try {

    /* A box around California. */
    lazy val queryExtent = Extent(-124.62890625, 32.32427558887655, -113.9501953125, 42.16340342422401)

    val layer: MultibandTileLayerRDD[SpatialKey] =
      env.reader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](
        LayerId(env.layerName, 0),
        env.numPartitions)

    val result =
      if (env.tiny)
        layer.filter().where(Intersects(queryExtent)).result
      else
        layer

    result.withContext(_.partitionBy(env.partitioner))
  }

  def tobler(srtm: MultibandTileLayerRDD[SpatialKey]): TileLayerRDD[SpatialKey] = {
    val elevation = srtm.withContext(_.mapValues(_.band(0)))
    Slope.fromWGS84(elevation)
      .withContext { rdd =>
        rdd.mapValues { tile =>
          tile.mapDouble { z =>
            val radians = z * math.Pi / 180.0
            val m = math.tan(radians)
            6 * (math.pow(math.E, (-3.5 * math.abs(m + 0.05))))
          }.interpretAs(FloatConstantNoDataCellType)
        }
      }.mapContext{
        _.copy(cellType = FloatConstantNoDataCellType)
      }
  }

  /** Overlay some OSM data onto the Tobler layer. */
  def osmLayer(
    env: Env,
    tobler: TileLayerRDD[SpatialKey]
  )(implicit ss: SparkSession): Try[TileLayerRDD[SpatialKey]] = {

    /* ORC file is assumed to be a snapshot with the most recent versions of every Element. */
    osm.fromORC(env.orcPath).map { case (ns, ws, _) =>

      val roadsAndWater: RDD[(Long, osm.Way)] = ws.repartition(env.numPartitions / 2).filter { case (_, w) =>
        w.meta.tags.contains("highway") || w.meta.tags.contains("waterway")
      }

      val features: osm.Features =
        osm.features(ns.repartition(env.numPartitions / 2), roadsAndWater, ss.sparkContext.emptyRDD)

      val fused: RDD[Feature[Geometry, osm.ElementMeta]] =
        ss.sparkContext.union(features.lines.map(identity), features.polygons.map(identity))

      /* The `CellValue`s given here will place roads above water in the rasterized
       * Tile, with the assumption that some bridge went overtop.
       */
      val lines: RDD[Feature[Geometry, CellValue]] = fused.map {
        case Feature(g, m) if m.tags.contains("highway") => Feature(g, CellValue(2, 2))
        case Feature(g, _) => Feature(g, CellValue(1, 1))  /* Assume it's a waterway */
      }

      /* Silently dumps the `Metadata` portion of the returned value.
       * It's just a `LayoutDefinition` that we borrowed from `tobler`
       * anyway, so we don't need it.
       */
      val geomTiles: RDD[(SpatialKey, Tile)] = lines.rasterize(ByteCellType, tobler.metadata.layout,
                                                               partitioner = Some(env.partitioner))

      val merged: RDD[(SpatialKey, Tile)] = tobler.leftOuterJoin(geomTiles).mapValues {
        case (v, None)    => v                 /* A Tile that had no overlain Geometries */
        case (v, Some(w)) => v.combineDouble(w) {
          case (vp, wp) if isNoData(wp) => vp  /* No overlain Geometry on this pixel */
          case (vp, 2) => vp * 0.8             /* Road detected: Reduce friction by 20% */
          case (vp, 1) => Double.NaN           /* Water detected: Set infinite friction */
          case (vp, _) => vp
        }
      }
      merged.cache()
      ContextRDD(merged, tobler.metadata)
    }
  }
}
