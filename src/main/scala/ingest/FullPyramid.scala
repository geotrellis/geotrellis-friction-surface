package ingest

import geotrellis.raster._
import geotrellis.raster.reproject.{ Reproject, ReprojectRasterExtent }
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index._
import geotrellis.spark.pyramid._
import geotrellis.spark.reproject._

import eu.timepit.refined.auto._
import org.apache.spark._

import scala.util.Try

object FullPyramid {
  def apply(env: Env, overlay: TileLayerRDD[SpatialKey]): Try[Unit] = Try {
    // Determine target cell size using same logic as GDAL
    val dataRasterExtent: RasterExtent = overlay.metadata.layout.createAlignedRasterExtent(overlay.metadata.extent)
    val targetRasterExtent = ReprojectRasterExtent(dataRasterExtent,
                                                   src = overlay.metadata.crs,
                                                   dest = env.layoutScheme.crs)

    println(s"Reprojecting to: ${targetRasterExtent.cellSize}")

    val (zoom, tiles) = TileRDDReproject(
      rdd = overlay,
      destCrs = env.layoutScheme.crs,
      targetLayout = Left(env.layoutScheme),
      bufferSize = 5,
      options=Reproject.Options(
        method = geotrellis.raster.resample.Bilinear,
        targetCellSize = Some(targetRasterExtent.cellSize)))

    Pyramid.levelStream(tiles, env.layoutScheme, zoom, 0).foreach { case (z, layer) =>
      val lid = LayerId(env.resultName, z)
      if (env.writer.attributeStore.layerExists(lid)) env.writer.attributeStore.delete(lid)
      env.writer.write(lid, layer, ZCurveKeyIndexMethod)
    }
  }
}
