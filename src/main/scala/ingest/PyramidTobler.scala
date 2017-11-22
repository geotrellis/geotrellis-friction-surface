package ingest

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject.{ Reproject, ReprojectRasterExtent}
import geotrellis.raster.io._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.kryo._
import geotrellis.spark.io.s3._
import geotrellis.spark.io.file._
import geotrellis.spark.io.index._
import geotrellis.spark.tiling._
import geotrellis.spark.pyramid._
import geotrellis.spark.reproject._
import geotrellis.vector._

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.serializer.KryoSerializer

object ToblerPyramid {
  def main(args: Array[String]): Unit = {

    val bucket = "geotrellis-test"
    val prefix = "dg-srtm"
    val catalog = s"s3://${bucket}/${prefix}"

    val layerName = "srtm-wsg84-gps"

    val resultName = "tobler-tiles-2"
    val layoutScheme = ZoomedLayoutScheme(WebMercator)

    val numPartitions =
      if(args.length > 0) {
        args(0).toInt
      } else {
        5000
      }
    val executors =
      if (args.length > 1) {
        args(1)
      } else {
        "51"
      }

    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setAppName("Ingest DEM")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
      .set("spark.driver-memory", "10G")
      .set("spark.driver.cores", "4")
      .set("spark.executor.instances", executors)
      .set("spark.executor.memory", "9472M") // XXX
      .set("spark.executor.cores", "4")
      .set("spark.yarn.executor.memoryOverhead","2G") // XXX
      .set("spark.driver.maxResultSize", "3G") // XXX

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

      val srtm = {
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

      val tobler =
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
      val targetRasterExtent = ReprojectRasterExtent(dataRasterExtent,
                                                     src = tobler.metadata.crs,
                                                     dest = layoutScheme.crs)

      println(s"Reprojecting to: ${targetRasterExtent.cellSize}")
      val (zoom, tiles) = TileRDDReproject(
        rdd = tobler,
        destCrs = layoutScheme.crs,
        targetLayout = Left(layoutScheme),
        bufferSize = 5,
        options=Reproject.Options(
          method = geotrellis.raster.resample.Bilinear,
          targetCellSize = Some(targetRasterExtent.cellSize)))

      Pyramid.levelStream(tiles, layoutScheme, zoom, 0).foreach { case (z, layer) =>
        val lid = LayerId(resultName, z)
        if (attributeStore.layerExists(lid)) attributeStore.delete(lid)
        layerWriter.write(lid, layer, ZCurveKeyIndexMethod)
      }
    } finally {
      sc.stop()
    }
  }
}
