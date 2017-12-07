package ingest

/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import geotrellis.spark._
import geotrellis.spark.io.index._
import geotrellis.spark.io.index.zcurve.{Z3, Z2, ZSpatialKeyIndex}
import geotrellis.util._

import org.apache.spark._
import org.apache.spark.rdd.{ShuffledRDD, RDD}

import scala.collection.mutable.ArrayBuffer
import scala.reflect._

/** Spatial partitioner for keys with SpatialComponent
  *
  * Places keys which share n bits of their Z2 index into same partition.
  * Partitioner index is modulo division of remaining bits by partition count.
  */
class ZPartitioner[K: SpatialComponent](partitions: Int, bits: Int = 8) extends Partitioner {

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    val SpatialKey(col, row) = k.getComponent[SpatialKey]
    ((Z2(col, row).z >> bits) % partitions).toInt
  }
}
