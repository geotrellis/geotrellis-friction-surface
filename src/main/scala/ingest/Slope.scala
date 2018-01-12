package ingest

import geotrellis.raster._
import geotrellis.raster.mapalgebra.focal.{Square, Slope => TileSlope}
import geotrellis.spark._
import geotrellis.spark.buffer._

object Slope {
  /** Calculate slope on elevation raster in meters in WSG84/LatLng raster
    * Special handling is required because the size of the cell changes with latitude.
    * Each tile will havei its own z-factor for slope calculation based on its latitude.
    */
  def fromWGS84(elevation: TileLayerRDD[SpatialKey]): TileLayerRDD[SpatialKey] = {
    val zfactorCalculator = (lat: Double) => 1 / (11320 * math.cos(math.toRadians(lat)))
    val cellSize = elevation.metadata.cellSize
    val mt = elevation.metadata.mapTransform

    elevation.withContext{ rdd =>
      rdd.bufferTiles(bufferSize = 1).mapPartitions[(SpatialKey, Tile)](
        { iter =>
          iter.map { case (key, BufferedTile(tile, bounds)) =>
            val tileCenter = mt.keyToExtent(key).center
            val zfactor = zfactorCalculator(tileCenter.y)
            key -> TileSlope(tile, Square(1), Some(bounds), cellSize, zfactor).interpretAs(FloatConstantNoDataCellType)
          }
        },
        preservesPartitioning = true
      )
    }.mapContext(_.copy(cellType = FloatConstantNoDataCellType))
  }
}
