package partitioner

import scala.annotation.tailrec

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
case class PartitioningInfo(
  partitions: Int,
  cut: Int,
  sCut: Int,
  level: Double,
  heavyKeys: Seq[(Any, Double)],
  partitionHistogram: Option[Map[Int, Double]] = None) {
  val sortedKeys: Array[Any] = heavyKeys.map(_._1).toArray
  val sortedValues: Array[Double] = heavyKeys.map(_._2).toArray

  override def toString: String = {
    s"PartitioningInfo [numberOfPartitions=$partitions, cut=$cut, sCut=$sCut, " +
      s"level=$level, heavyKeys = ${heavyKeys.take(10).mkString("[", ", ", "...]")}]"
  }
}

// Create PartitioningInfo from global key histogram
object PartitioningInfo {
  def newInstance(globalHistogram: scala.collection.Seq[(Any, Double)], numPartitions: Int,
    treeDepthHint: Int, sCutHint: Int = 0, partitionHistogram: Option[Map[Int, Double]] = None): PartitioningInfo = {
    require(numPartitions > 0, "Where's my number of partitions, I can not call you maybe!")

    val sortedValues = globalHistogram.map(_._2).toArray.take(numPartitions)
    val pCutHint = Math.pow(2, treeDepthHint - 1).toInt
    val startingCut = Math.min(numPartitions, sortedValues.length)
    var computedSCut = 0
    var computedLevel = 1.0d / numPartitions
    var remainder = 1.0d

    @tailrec
    def computeCuts(i: Int): Unit = {
      if (i < startingCut && computedLevel <= sortedValues(i)) {
        remainder -= sortedValues(i)
        if (i < numPartitions - 1) computedLevel = remainder / (numPartitions - 1 - i) else computedLevel = 0.0d
        computedSCut += 1
        computeCuts(i + 1)
      }
    }

    computeCuts(0)

    val actualSCut = Math.max(sCutHint, computedSCut)
    val actualPCut = Math.min(pCutHint, startingCut - actualSCut)
    /**
      * Recompute level to minimize rounding errors.
      */
    val level = Math.max(0, (1.0d - sortedValues.take(actualSCut).sum) / (numPartitions - actualSCut))
    val actualCut = actualSCut + actualPCut
    PartitioningInfo(numPartitions, actualCut, actualSCut, level, globalHistogram, partitionHistogram)
  }
}