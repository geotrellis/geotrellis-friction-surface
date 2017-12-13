package ingest

import geotrellis.raster._
import geotrellis.raster.mapalgebra.focal.{Square, Slope => TileSlope}
import geotrellis.spark._
import geotrellis.spark.buffer._
import org.apache.commons.math.analysis.interpolation._

object Slope {
  /** Calculate slope on elevation raster in meters in WSG84/LatLng raster
    * Special handling is required because the size of the cell changes with latitude.
    * Each tile will havei its own z-factor for slope calculation based on its latitude.
    */
  def fromWGS84(elevation: TileLayerRDD[SpatialKey]): TileLayerRDD[SpatialKey] = {
    val lattitude = Array[Double](0, 10, 20, 30, 40, 50, 60, 70, 80)
    val zfactors = Array[Double](0.00000898, 0.00000912, 0.00000956, 0.00001036, 0.00001171, 0.00001395, 0.00001792, 0.00002619, 0.00005156)
    val cellSize = elevation.metadata.cellSize
    val mt = elevation.metadata.mapTransform

    elevation.withContext{ rdd =>
      rdd.bufferTiles(bufferSize = 1).mapPartitions[(SpatialKey, Tile)](
        { iter =>
          val interp = new LinearInterpolator()
          val spline = interp.interpolate(lattitude, zfactors)
          iter.map { case (key, BufferedTile(tile, bounds)) =>
            val tileCenter = mt.keyToExtent(key).center
            val zfactor = spline.value(tileCenter.y)
            key -> TileSlope(tile, Square(1), Some(bounds), cellSize, zfactor).interpretAs(FloatConstantNoDataCellType)
          }
        },
        preservesPartitioning = true
      )
    }.mapContext(_.copy(cellType = FloatConstantNoDataCellType))
  }
}
