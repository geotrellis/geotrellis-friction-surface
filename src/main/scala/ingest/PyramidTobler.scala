package ingest

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject.{ Reproject, ReprojectRasterExtent }
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
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
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer

object ToblerPyramid extends CommandApp(

  name   = "tobler-ingest",
  header = "Perform the Tobler Ingest",
  main   = {

    /* Ensures that only positive, non-zero values can be given as arguments. */
    type UInt = Int Refined Positive

    val partO: Opts[UInt] = Opts.option[UInt]("partitions", help = "Spark partitions to use.").withDefault(5000)
    val execO: Opts[UInt] = Opts.option[UInt]("executors",  help = "Spark executors to use.").withDefault(50)

    (partO, execO).mapN { (numPartitions, executors) =>

    val bucket = "geotrellis-test"
    val prefix = "dg-srtm"
    val catalog = s"s3://${bucket}/${prefix}"
    val layerName = "srtm-wsg84-gps"
    val resultName = "tobler-tiles-5"
    val layoutScheme = ZoomedLayoutScheme(WebMercator)

    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setAppName("Ingest DEM")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
      .set("spark.driver-memory", "10G")
      .set("spark.driver.cores", "4")
      .set("spark.executor.instances", executors.value.show)
      .set("spark.executor.memory", "9472M") // XXX
      .set("spark.executor.cores", "4")
      .set("spark.yarn.executor.memoryOverhead","2G") // XXX
      .set("spark.driver.maxResultSize", "3G") // XXX
      .set("spark.shuffle.compress", "true")
      .set("spark.shuffle.spill.compress", "true")
      .set("spark.rdd.compress", "true")
      .set("spark.task.maxFailures", "33")

    implicit val sc = new SparkContext(conf)

    try {
      val layerReader = S3LayerReader(bucket, prefix)
      val layerWriter = if (conf.get("spark.master").startsWith("local"))
                          FileLayerWriter("/tmp/dg-srtm")
                        else
                          S3LayerWriter(bucket, prefix)

      val attributeStore = layerWriter.attributeStore

      val queryExtent = Extent(
        -120.36209106445312,38.8407772667165,
        -119.83612060546874,39.30242456041487)

      val srtm: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
        val temp =
          layerReader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](
            LayerId(layerName, 0),
            numPartitions=numPartitions)

        if (conf.get("spark.master").startsWith("local"))
          temp
            .filter()
            .where(Intersects(queryExtent))
            .result
        else
          temp
      }

      val tobler: TileLayerRDD[SpatialKey] =
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

      // Determine target cell size using same logic as GDAL
      val dataRasterExtent: RasterExtent = tobler.metadata.layout.createAlignedRasterExtent(tobler.metadata.extent)
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
              src = tobler.metadata.crs,
              dest = layoutScheme.crs)

          println(s"Reprojecting to: ${targetRasterExtent.cellSize}")

          val toblerSubset: TileLayerRDD[SpatialKey] = ContextRDD(
            tobler.filter().where(Intersects(extent)).result,
            tobler.metadata)

          val (zoom, tiles): (Int, TileLayerRDD[SpatialKey]) = TileRDDReproject(
            rdd = toblerSubset,
            destCrs = layoutScheme.crs,
            targetLayout = Left(layoutScheme),
            bufferSize = 5,
            options=Reproject.Options(
              method = geotrellis.raster.resample.Bilinear,
              targetCellSize = Some(targetRasterExtent.cellSize)))

          Pyramid.levelStream(tiles, layoutScheme, zoom, 0).foreach { case (z, layer) =>
            val lid = LayerId(resultName, z)
            if (i === 0 && j === 0) {
              if (attributeStore.layerExists(lid))
                attributeStore.delete(lid)
              layerWriter.write(lid, layer, ZCurveKeyIndexMethod)
            }
            else {
              if (!attributeStore.layerExists(lid))
                layerWriter.write(lid, layer, ZCurveKeyIndexMethod)
              else
                layerWriter.update(lid, layer, {(t: Tile, _: Tile) => t})
            }
          }
          j += 1
        }
        i += 1
      }
    } finally {
      sc.stop()
    }
  }}
)
