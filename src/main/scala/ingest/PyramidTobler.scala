package ingest

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.pyramid._
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
import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer
import org.apache.log4j.Logger


object CountToblerLogger {
  def getCountLogger = Logger.getLogger(CountTobler.getClass)
}

object CountTobler extends CommandApp(

  name   = "tobler-ingest",
  header = "Perform the Tobler Ingest",
  main   = {

    /* Ensures that only positive, non-zero values can be given as arguments. */
    type UInt = Int Refined Positive

    val partO: Opts[UInt]    = Opts.option[UInt]("partitions", help = "Spark partitions to use.").withDefault(15000)
    val pathO: Opts[String]  = Opts.option[String]("orc",      help = "Path to an ORC file to overlay.")
    val outpO: Opts[String]  = Opts.option[String]("layer",    help = "Name of the output layer.")
    val tinyF: Opts[Boolean] = Opts.flag("tiny", help = "Constrain the job to only run on California.").orFalse

    (partO, pathO, outpO, tinyF).mapN { (numPartitions, orcPath, resultName, tiny) =>

      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("Tobler Ingest")
        .set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)

      implicit val ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate

      val bucket        = "geotrellis-test"
      val prefix        = "dg-srtm"
      val layerName     = "srtm-wsg84-gps"
      val layoutScheme  = ZoomedLayoutScheme(WebMercator)
      val reader        = S3LayerReader(bucket, prefix)(ss.sparkContext)
      val writer        = S3LayerWriter(bucket, prefix)
      val partitions    = numPartitions.value
      val bbox =
        if (tiny)
          Some(Extent(-124.62890625, 32.32427558887655, -113.9501953125, 42.16340342422401))
        else
          None

      val logger = CountToblerLogger.getCountLogger

      try {
        val elevation = Work.elevationLayer(reader, layerName, partitions, bbox)
        val tobler = Work.toblerLayer(elevation)
        val osm = Work.osmLayer(orcPath, tobler.metadata.layout, partitions/2, new ZPartitioner(numPartitions.value))
        val merged = Work.mergeLayers(tobler, osm)
        val (baseZoom, baseLayer) = Work.reproject(merged, layoutScheme)

        val pyramid: Stream[(Int, TileLayerRDD[SpatialKey])] =
          Pyramid.levelStream(baseLayer, layoutScheme, baseZoom, 0)

        pyramid.foreach { case (z, layer) => logger.debug(s"ZOOM: $z, COUNT: ${layer.count}") }

      } finally {
        ss.stop
      }
    }
  }
)


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

    (partO, pathO, outpO, tinyF).mapN { (numPartitions, orcPath, resultName, tiny) =>

      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("Tobler Ingest")
        .set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)

      implicit val ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate

      val bucket        = "geotrellis-test"
      val prefix        = "dg-srtm"
      val layerName     = "srtm-wsg84-gps"
      val layoutScheme  = ZoomedLayoutScheme(WebMercator)
      val reader        = S3LayerReader(bucket, prefix)(ss.sparkContext)
      val writer        = S3LayerWriter(bucket, prefix)
      val partitions    = numPartitions.value
      val bbox =
        if (tiny)
          Some(Extent(-124.62890625, 32.32427558887655, -113.9501953125, 42.16340342422401))
        else
          None

      try {
        val elevation = Work.elevationLayer(reader, layerName, partitions, bbox)
        val tobler = Work.toblerLayer(elevation)
        val osm = Work.osmLayer(orcPath, tobler.metadata.layout, partitions/2, new ZPartitioner(numPartitions.value))
        val merged = Work.mergeLayers(tobler, osm)
        val (baseZoom, baseLayer) = Work.reproject(merged, layoutScheme)

        val pyramid: Stream[(Int, TileLayerRDD[SpatialKey])] =
          Pyramid.levelStream(baseLayer, layoutScheme, baseZoom, 0)

        Work.savePyramid(resultName, pyramid, writer)

      } finally {
        ss.stop
      }
    }
  }
)
