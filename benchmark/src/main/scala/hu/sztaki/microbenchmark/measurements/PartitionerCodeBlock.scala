package hu.sztaki.microbenchmark.measurements

import PartitionerUpdateMeasurement.getPartitionHistogram
import hu.sztaki.microbenchmark
import hu.sztaki.microbenchmark.utils.{CodeBlock, Distribution}
import hu.sztaki.microbenchmark.partitioner.Partitioner.PartitionerType
import hu.sztaki.microbenchmark.partitioner.Partitioner.PartitionerType.PartitionerType
import hu.sztaki.microbenchmark.partitioner.{PartitioningInfo, Updateable}
import hu.sztaki.microbenchmark.utils.ValueGenerator.{KeyedRecordGenerator, ValueTuple}

import scala.reflect.ClassTag
import scala.util.Random

abstract class PartitionerCodeBlock[T](
                                        val distribution: Distribution,
                                        val keyedRecordGenerator: KeyedRecordGenerator[T, ValueTuple],
                                        val partitionerType: PartitionerType,
                                        val numBatches: Int,
                                        val batchSize: Int,
                                        val numPartitions: Int,
                                        val numKeys: Int,
                                        val keyExcess: Int,
                                        val thetaMax: Double
                                      )(implicit val evidence: ClassTag[T])
  extends CodeBlock {
  // informations needed to construct the partitioner
  var partitioningInfo: PartitioningInfo[T] = _
  var partitioner: Updateable[T] = _
  // total list of keys
  var keys: Array[T] = _
  // key histogram as a list of (key, frequency) pairs
  var keyHistogram: Seq[(T, Double)] = _
  var partitionHistogram: Map[Int, Double] = _
  // measurement counter, goes from 0 to iterations * (numBatches - 1)
  var i: Int = 0

  def initPartitiner(): Unit = {
    // if a new iteration of the measurement has started, reinitialize the partitioner
    if (i % numBatches == 0) {
      partitioner = microbenchmark.partitioner.Updateable[T](partitionerType, numPartitions, keyExcess, thetaMax)
      // generate a new set of keys with transformKey
      keys = (1 to numKeys).map(k => keyedRecordGenerator.transformKey(k, Random.nextInt)).toArray
    }
  }

  def initPartitioningInfo(): Unit = {
    // generate a new (sorted) key histogram for the current batch
    keyHistogram = keys.zip(distribution.unorderedEmpiric(batchSize)).filter(_._2 > 0.0d).sortBy(-_._2)

    // partitioner parameter, currently out of use
    val treeDepthHint = 100

    partitionerType match {
      case PartitionerType.Mixed =>
        // calculate the partitioning histogram for the current batch
        partitionHistogram = getPartitionHistogram(partitioner, keyHistogram)
        // calculate the corresponding partitioning info
        partitioningInfo = PartitioningInfo[T](keyHistogram, numPartitions, treeDepthHint, partitionHistogram = Some(partitionHistogram))

      // in case of partitioners other than Mixed, we don't need to calculate the
      // partitioning histogram
      case _ =>
        // calculate the corresponding partitioning info
        partitioningInfo = PartitioningInfo[T](keyHistogram.take(keyExcess * numPartitions + 1), numPartitions, treeDepthHint)
    }
  }

  // code to be measured; called immediately after init()
  override def compute(): Any = {
    // update the partitioner
    partitioner = partitioner.update(partitioningInfo)
    partitioner
  }
}
