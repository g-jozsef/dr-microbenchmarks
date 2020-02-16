package partitioner

import scala.annotation.tailrec
import scala.reflect.ClassTag
import utils.computeIf

/**
  * Informations required to construct the partitioner
  *
  * @param partitions         number of partitions
  * @param cut                UNUSED
  * @param sCut               UNUSED
  * @param level              optimal minimal size of partitions; the partitioner will try to fill up every
  *                           partition with lightweight keys up to this level
  * @param heavyKeys          sequence of heavy keys
  * @param partitionHistogram distribution of partition sizes in the previous batch; not all
  *                           partitioner needs this information
  */
case class PartitioningInfo[T](
                                partitions: Int,
                                cut: Int,
                                sCut: Int,
                                level: Double,
                                heavyKeys: Seq[(T, Double)],
                                partitionHistogram: Option[Map[Int, Double]] = None)(implicit tag: ClassTag[T]) {
  val sortedKeys: Array[T] = heavyKeys.map(_._1).toArray
  val sortedValues: Array[Double] = heavyKeys.map(_._2).toArray

  override def toString: String = {
    s"PartitioningInfo [numberOfPartitions=$partitions, cut=$cut, sCut=$sCut, " +
      s"level=$level, heavyKeys = ${heavyKeys.take(10).mkString("[", ", ", "...]")}]"
  }
}

// Create PartitioningInfo from global key histogram
object PartitioningInfo {
  def newInstance[T](globalHistogram: scala.collection.Seq[(T, Double)], numPartitions: Int,
                     treeDepthHint: Int, sCutHint: Int = 0, partitionHistogram: Option[Map[Int, Double]] = None)(implicit tag: ClassTag[T]): PartitioningInfo[T] = {
    require(numPartitions > 0, "Where's my number of partitions, I can not call you maybe!")

    val sortedValues = globalHistogram.map(_._2).toArray.take(numPartitions)
    val pCutHint = Math.pow(2, treeDepthHint - 1).toInt
    val startingCut = Math.min(numPartitions, sortedValues.length)
    var remainder = 1.0d
    var computedLevel = remainder / numPartitions

    val computedSCut = ((0 until startingCut) zip sortedValues).takeWhile({
      case (i: Int, sortedValue: Double) => {
        computeIf(computedLevel <= sortedValue, () => {
          remainder -= sortedValue
          computedLevel =
            if (i < numPartitions - 1)
              remainder / (numPartitions - 1 - i)
            else
              0.0d
        })
      }
    }).length

    val actualSCut = Math.max(sCutHint, computedSCut)
    val actualPCut = Math.min(pCutHint, startingCut - actualSCut)

    /**
      * Recompute level to minimize rounding errors.
      */
    val level = Math.max(0, (1.0d - sortedValues.take(actualSCut).sum) / (numPartitions - actualSCut))
    val actualCut = actualSCut + actualPCut
    PartitioningInfo[T](numPartitions, actualCut, actualSCut, level, globalHistogram, partitionHistogram)
  }
}