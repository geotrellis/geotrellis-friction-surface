package ingest


import geotrellis.raster._
import geotrellis.raster.io._
import geotrellis.raster.rasterize.CellValue
import geotrellis.raster.render._
import geotrellis.spark._
import geotrellis.spark.reproject._
import geotrellis.spark.io._
import geotrellis.spark.io.index._
import geotrellis.spark.io.s3._
import geotrellis.spark.tiling._
import geotrellis.raster.reproject.{ Reproject, ReprojectRasterExtent }
import geotrellis.vector._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import vectorpipe._


object Work {

  /** Read elevation layer, query to bounding box is given */
  def elevationLayer(
    reader: FilteringLayerReader[LayerId],
    layerName: String,
    numPartitions: Int,
    bbox: Option[Extent] = None
  ): MultibandTileLayerRDD[SpatialKey] = {
    (bbox match {
      case Some(queryExtent) =>
        reader
          .query[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](LayerId(layerName, 0), numPartitions)
          .where(Intersects(queryExtent))
          .result
      case None =>
        reader
          .read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](LayerId(layerName, 0), numPartitions)
     }).withContext { rdd =>
      // adjust for mishandled NODATA value on ingest
      rdd.mapValues(_.withNoData(Some(0)))
    }.mapContext { md =>
      // adjust layer metadata to reflect per-tile changes
      md.copy(cellType = md.cellType.withNoData(Some(0)))
    }
  }

  /** Calculate Tolber Hicking function based on first band in layer */
  def toblerLayer(srtm: MultibandTileLayerRDD[SpatialKey]): TileLayerRDD[SpatialKey] = {
    val elevation = srtm.withContext(_.mapValues(_.band(0)))
    Slope.fromWGS84(elevation)
      .withContext { rdd =>
        rdd.mapValues { tile =>
          tile.mapDouble { z =>
            val radians = z * math.Pi / 180.0
            val m = math.tan(radians)
            6 * (math.pow(math.E, (-3.5 * math.abs(m + 0.05))))
          }
        }
      }
  }

  /** Read and rasterize OSM highways and waterways */
  def osmLayer(
    orcPath: String,
    layout: LayoutDefinition,
    numPartitions: Int,
    partitioner: Partitioner
  )(implicit ss: SparkSession): RDD[(SpatialKey, Tile)] with Metadata[LayoutDefinition] = {

    /* ORC file is assumed to be a snapshot with the most recent versions of every Element. */
    osm.fromORC(orcPath).map { case (ns, ws, _) =>

      val roadsAndWater: RDD[(Long, osm.Way)] = ws.repartition(numPartitions).filter { case (_, w) =>
        w.meta.tags.contains("highway") || w.meta.tags.contains("waterway")
      }

      val features: osm.Features =
        osm.features(ns.repartition(numPartitions), roadsAndWater, ss.sparkContext.emptyRDD)

      val fused: RDD[Feature[Geometry, osm.ElementMeta]] =
        ss.sparkContext.union(features.lines.map(identity), features.polygons.map(identity))

      /* The `CellValue`s given here will place roads above water in the rasterized
       * Tile, with the assumption that some bridge went overtop.
       */
      val lines: RDD[Feature[Geometry, CellValue]] = fused.flatMap {
        case Feature(g, m) if m.tags.contains("highway") =>
          /* Use speed as both value and z-order for rasterizer
           * Drop records where maxspeed can't be parsed
           */
          OsmHighwaySpeed(m).map { maxspeed =>
            Feature(g, CellValue(maxspeed, maxspeed))
          }

        case _ =>
          None
      }

      /* Silently dumps the `Metadata` portion of the returned value.
       * It's just a `LayoutDefinition` that we borrowed from `tobler`
       * anyway, so we don't need it.
       */
      lines.rasterize(ByteCellType, layout, partitioner = Some(partitioner))
    }.get
  }

  /** Merge rasterized OSM layer with tobler */
  def mergeLayers(
    tobler:  TileLayerRDD[SpatialKey],
    osm: RDD[(SpatialKey, Tile)] with Metadata[LayoutDefinition]
  ):  TileLayerRDD[SpatialKey]= {
    val merged =
      tobler.leftOuterJoin(osm).mapValues {
        case (v, None)    => v                 /* A Tile that had no overlain Geometries */
        case (v, Some(w)) => v.combineDouble(w) {
          case (vp, wp) if isNoData(wp) => vp  /* No overlain Geometry on this pixel */
          case (vp, wp) => math.max(vp, wp)    /* Use maximum speed between tobler and highway layer */
        }
      }
    merged.cache()
    ContextRDD(merged, tobler.metadata)
  }

  /** Reproject layer to WebMercator pyramid for visualization */
  def reproject(overlay: TileLayerRDD[SpatialKey], layoutScheme: ZoomedLayoutScheme): (Int, TileLayerRDD[SpatialKey]) = {
    // Determine target cell size using same logic as GDAL
    val dataRasterExtent: RasterExtent = overlay.metadata.layout.createAlignedRasterExtent(overlay.metadata.extent)
    val targetRasterExtent =
      ReprojectRasterExtent(dataRasterExtent, src = overlay.metadata.crs, dest = layoutScheme.crs)

    TileRDDReproject(
      rdd = overlay,
      destCrs = layoutScheme.crs,
      targetLayout = Left(layoutScheme),
      bufferSize = 5,
      options=Reproject.Options(
        method = geotrellis.raster.resample.Bilinear,
        targetCellSize = Some(targetRasterExtent.cellSize)))
  }

  /** Save layer pyramid to GeoTrellis Avro catalog */
  def savePyramid(resultName: String, pyramid: Stream[(Int, TileLayerRDD[SpatialKey])], writer: LayerWriter[LayerId]): Unit = {
    pyramid.head._2.cache()
    val histogram = pyramid.head._2.histogram()
    writer.attributeStore.write(LayerId(resultName,0), "histogram", histogram)

    pyramid.foreach { case (z, layer) =>
      val lid = LayerId(resultName, z)
      if (writer.attributeStore.layerExists(lid)) writer.attributeStore.delete(lid)
      writer.write(lid, layer, ZCurveKeyIndexMethod)
    }
  }

  /** Render layer pyramid as PNG to S3 bucket */
  def renderPyramid(url: String, name: String, pyramid: Stream[(Int, TileLayerRDD[SpatialKey])]): Unit = {
    pyramid.head._2.cache()
    val histogram = pyramid.head._2.histogram(256)

    pyramid.foreach { case (z, layer) =>
      val colorMap = ColorMap.fromQuantileBreaks(histogram, ColorRamps.Inferno)
      layer.mapValues(_.renderPng(colorMap).bytes).saveToS3(key =>
        s"$url/${name}/${z}/${key.col}/${key.row}.png")
    }
  }
}
