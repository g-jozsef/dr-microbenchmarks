package partitioner

import partitioner.GedikPartitioner.GedikPartitioner
import partitioner.Partitioner.PartitionerType.PartitionerType

import scala.reflect.ClassTag

trait Partitioner[T] {
  def numPartitions: Int

  def getPartition(key: T): Int
}

object Partitioner {

  object PartitionerType extends Enumeration {
    type PartitionerType = Value
    val Scan, Redist, Readj, KeyIsolator, Mixed, Hash = Value
  }

  def apply[T](pType: PartitionerType, numPartitions: Int, keyExcess: Int, thetaMax: Double): Partitioner[T] = {
    pType match {
      case PartitionerType.Hash =>
        new HashPartitioner(numPartitions)
      case other =>
        Updateable(other, numPartitions, keyExcess, thetaMax)
    }
  }

  def update[T](partitioner: Updateable[T], weights: Array[(T, Double)], keyExcess: Int, numPartitions: Int)(implicit tag: ClassTag[T]): Partitioner[T] = {
    // partitioner parameter, currently out of use
    val treeDepthHint = (Math.log10(1000) / Math.log10(2.0d)).toInt + 2
    partitioner match {
      case _: GedikPartitioner[T] | _: KeyIsolatorPartitioner[ConsistentHashPartitioner[T], T] =>
        // informations needed to construct the partitioner
        val partitioningInfo: PartitioningInfo[T] = PartitioningInfo[T](weights.take(keyExcess * numPartitions + 1), numPartitions, treeDepthHint)

        partitioner.update(partitioningInfo)
      case _: MixedPartitioner[T] =>

        // calculate the partition histogram
        // we need to know the partition histogram for the update
        var partitionHistogram = (0 until numPartitions).map(p => p -> 0.0d).toMap
        weights.foreach {
          case (key, weight) =>
            val part = partitioner.getPartition(key)
            partitionHistogram = partitionHistogram + (part -> (partitionHistogram(part) + weight))
        }
        // Mixed partitioner needs a different partitioning info
        val partitioningInfo: PartitioningInfo[T] = PartitioningInfo[T](weights, numPartitions, treeDepthHint,
          partitionHistogram = Some(partitionHistogram))

        // update Mixed partitioner
        partitioner.update(partitioningInfo)

      case _ =>
        partitioner
    }
  }
}