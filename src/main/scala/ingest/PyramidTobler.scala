package ingest

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject.{ Reproject, ReprojectRasterExtent }
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index._
import geotrellis.spark.io.kryo._
import geotrellis.spark.io.s3._
import geotrellis.spark.pyramid._
import geotrellis.spark.reproject._
import geotrellis.spark.tiling._
import geotrellis.vector._

import cats.implicits._
import com.monovore.decline._
import com.monovore.decline.refined._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer
import vectorpipe._

import scala.util.{ Try, Success, Failure }

// --- //

case class Env(
  layerName: String,
  overlayName: String,
  resultName: String,
  layoutScheme: ZoomedLayoutScheme,
  numPartitions: Int,
  orcPath: String,
  tiny: Boolean,
  sparkConf: SparkConf,
  reader: LayerReader[LayerId],
  writer: LayerWriter[LayerId]
)

object ToblerPyramid extends CommandApp(

  name   = "tobler-ingest",
  header = "Perform the Tobler Ingest",
  main   = {

    /* Ensures that only positive, non-zero values can be given as arguments. */
    type UInt = Int Refined Positive

    val partO: Opts[UInt]    = Opts.option[UInt]("partitions", help = "Spark partitions to use.").withDefault(5000)
    val pathO: Opts[String]  = Opts.option[String]("orc",      help = "Path to an ORC file to overlay.")
    val tinyF: Opts[Boolean] = Opts.flag("tiny", help = "Constrain the job to only run on California.").orFalse

    (partO, pathO, tinyF).mapN { (numPartitions, orc, tiny) =>

      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("Ingest DEM")
        .set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)

      implicit val ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate

      val bucket = "geotrellis-test"
      val prefix = "dg-srtm"

      val env: Env = Env(
        layerName     = "srtm-wsg84-gps",
        overlayName   = "osm-overlay",
        resultName    = "tobler-overlay",
        layoutScheme  = ZoomedLayoutScheme(WebMercator),
        numPartitions = numPartitions.value,
        orcPath       = orc,
        tiny          = tiny,
        sparkConf     = conf,
        reader        = S3LayerReader(bucket, prefix)(ss.sparkContext),
        writer        = S3LayerWriter(bucket, prefix)
      )

      (Work.roadOverlay(env) >>= { Work.pyramid(env, _) }) match {
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
    val lid = LayerId(env.overlayName, 0)

    if (env.writer.attributeStore.layerExists(lid))
      Try(env.reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](lid, env.numPartitions))
    else
      sourceLayer(env).map(tobler) >>= { osmLayer(env, _) } >>= { saveOSM(env, _) }
  }

  /** Reduced in size if this is a local ingest. */
  def sourceLayer(env: Env): Try[MultibandTileLayerRDD[SpatialKey]] = Try {

    /* A box around California. */
    lazy val queryExtent = Extent(-124.62890625, 32.32427558887655, -113.9501953125, 42.16340342422401)

    val layer: MultibandTileLayerRDD[SpatialKey] =
      env.reader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](
        LayerId(env.layerName, 0),
        env.numPartitions)

    if (env.tiny)
      layer.filter().where(Intersects(queryExtent)).result
    else
      layer
  }

  def tobler(srtm: MultibandTileLayerRDD[SpatialKey]): TileLayerRDD[SpatialKey] = {
    srtm
      .withContext(_.mapValues(_.band(0)))
      .slope()
      .withContext { rdd =>
        rdd.mapValues { tile =>
          tile.mapDouble { z =>
            val radians = z * math.Pi / 180.0
            val tobler_kph = 6 * (math.pow(2.718281828, (-3.5 * math.abs(radians + 0.05))))
            3.6 / tobler_kph
          }
        }
      }
  }

  /** Overlay some OSM data onto the Tobler layer. */
  def osmLayer(
    env: Env,
    tobler: TileLayerRDD[SpatialKey]
  )(implicit ss: SparkSession): Try[TileLayerRDD[SpatialKey]] = {

    /* ORC file is assumed to be a snapshot with the most recent versions of every Element. */
    osm.fromORC(env.orcPath).map { case (ns, ws, _) =>
      val roads: RDD[(Long, osm.Way)] = ws.filter { case (_, w) => w.meta.tags.contains("highway") }

      val lines: RDD[Line] = osm.toHistory(ns, roads)._2.map(_.geom)

      /* Silently dumps the `Metadata` portion of the returned value.
       * It's just a `LayoutDefinition` that we borrowed from `tobler`
       * anyway, so we don't need it.
       */
      val geomTiles: RDD[(SpatialKey, Tile)] = lines.rasterize(1, BitCellType, tobler.metadata.layout)

      ContextRDD(geomTiles.merge(tobler), tobler.metadata)
    }
  }

  def saveOSM(env: Env, osm: TileLayerRDD[SpatialKey]): Try[TileLayerRDD[SpatialKey]] = {
    osm.persist(StorageLevel.MEMORY_AND_DISK_SER)

    Try(env.writer.write(LayerId(env.overlayName, 0), osm, ZCurveKeyIndexMethod)).map(_ => osm)
  }

  def pyramid(env: Env, overlay: TileLayerRDD[SpatialKey]): Try[Unit] = Try {
    // Determine target cell size using same logic as GDAL
    val dataRasterExtent: RasterExtent = overlay.metadata.layout.createAlignedRasterExtent(overlay.metadata.extent)
    val Extent(xmin, ymin, xmax, ymax) = dataRasterExtent.extent
    val cut = 3
    val width = xmax - xmin
    val height = ymax - ymin
    val subWidthLesser = width / cut
    val subHeightLesser = height / cut
    val subWidthGreater = width / (cut - 0.25)
    val subHeightGreater = height / (cut - 0.25)
    val cellSize = CellSize(dataRasterExtent.cellwidth, dataRasterExtent.cellheight)

    var i = 0; while (i < cut) {
      var j = 0; while (j < cut) {
        val xmin2 = xmin + (i * subWidthLesser)
        val xmax2 = xmin2 + subWidthGreater
        val ymin2 = ymin + (j * subHeightLesser)
        val ymax2 = ymin2 + subHeightGreater
        val extent = Extent(xmin2,  ymin2, xmax2, ymax2)
        val subDataRasterExtent = RasterExtent(extent, cellSize)

        val targetRasterExtent =
          ReprojectRasterExtent(
            subDataRasterExtent,
            src = overlay.metadata.crs,
            dest = env.layoutScheme.crs)

        println(s"Reprojecting to: ${targetRasterExtent.cellSize}")

        val toblerSubset: TileLayerRDD[SpatialKey] = ContextRDD(
          overlay.filter().where(Intersects(extent)).result,
          overlay.metadata)

        val (zoom, tiles): (Int, TileLayerRDD[SpatialKey]) = TileRDDReproject(
          rdd = toblerSubset,
          destCrs = env.layoutScheme.crs,
          targetLayout = Left(env.layoutScheme),
          bufferSize = 5,
          options=Reproject.Options(
            method = geotrellis.raster.resample.Bilinear,
            targetCellSize = Some(targetRasterExtent.cellSize)))

        Pyramid.levelStream(tiles, env.layoutScheme, zoom, 0).foreach { case (z, layer) =>
          val lid = LayerId(env.resultName, z)
          if (i === 0 && j === 0) {
            if (env.writer.attributeStore.layerExists(lid))
              env.writer.attributeStore.delete(lid)
            env.writer.write(lid, layer, ZCurveKeyIndexMethod)
          }
          else {
            if (!env.writer.attributeStore.layerExists(lid))
              env.writer.write(lid, layer, ZCurveKeyIndexMethod)
            else
              env.writer.update(lid, layer, {(t: Tile, _: Tile) => t})
          }
        }
        j += 1
      }
      i += 1
    }
  }
}
