package ingest

import geotrellis.raster._
import geotrellis.raster.reproject.{ Reproject, ReprojectRasterExtent }
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index._

import geotrellis.spark.pyramid._
import geotrellis.spark.reproject._

import geotrellis.vector._

import cats.implicits._
import eu.timepit.refined.auto._
import org.apache.spark._

import scala.util.Try

object CutPyramid {
  def apply(env: Env, overlay: TileLayerRDD[SpatialKey], cut: Int = 1): Try[Unit] = Try {
    // Determine target cell size using same logic as GDAL
    val dataRasterExtent: RasterExtent = overlay.metadata.layout.createAlignedRasterExtent(overlay.metadata.extent)
    val Extent(xmin, ymin, xmax, ymax) = dataRasterExtent.extent
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
