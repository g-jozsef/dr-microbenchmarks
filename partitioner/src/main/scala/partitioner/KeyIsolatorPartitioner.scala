package partitioner

import scala.collection.immutable.{HashMap, TreeSet}
import KeyIsolatorPartitioner._

class KeyIsolatorPartitioner[P <: Adaptive[P, T] with MigrationCostEstimator, T] private(
  override val numPartitions: Int,
  val heavyKeysMap: Map[T, Int] = Map[T, Int](),
  val internalPartitioner: Partitioner[T],
  val migrationCostEstimation: Option[(Double, Double)] = None,
  val initializeInternalPartitioner: (PartitioningInfo[T], Array[Double]) => P)
  extends Updateable[T] with MigrationCostEstimator {

  def this(
    numPartitions: Int,
    initializeInternalPartitioner: (PartitioningInfo[T], Array[Double]) => P) = {
    this(numPartitions,
      internalPartitioner = new HashPartitioner[T](numPartitions),
      initializeInternalPartitioner = initializeInternalPartitioner)
  }

  override def getPartition(key: T): Int = {
    heavyKeysMap.get(key) match {
      case Some(part) => part
      case None =>
        internalPartitioner.getPartition(key)
    }
  }

  override def getMigrationCostEstimation: Option[Double] =
    migrationCostEstimation.map({ case (hm, sm) => hm + sm })

  def getInternalMigrationCostEstimation: Option[Double] =
    migrationCostEstimation.map({ case (hm, sm) => sm })

  override def update(partitioningInfo: PartitioningInfo[T]): KeyIsolatorPartitioner[P, T] = {

    implicit val ordering: Ordering[(Int, Double)] = new Ordering[(Int, Double)] {
      override def compare(x: (Int, Double), y: (Int, Double)): Int = {
        val res0 = x._2.compareTo(y._2)
        if (res0 != 0) {
          res0
        } else {
          x._1.compareTo(y._1)
        }
      }
    }

    val allowedBalanceError = 0.001d

    val numHeavyKeys = Math.min((numPartitions * keyExcess).round.toInt, partitioningInfo.heavyKeys.size)
    // ordered by frequency
    val heavyKeysWithFrequencies = partitioningInfo.heavyKeys.take(numHeavyKeys)
    val heavyKeyToFrequencyMap: Map[T, Double] = heavyKeysWithFrequencies.toMap
    val allowedLevel = Math.max(heavyKeysWithFrequencies.head._2, 1.0d / numPartitions) +
      allowedBalanceError
    var explicitHash = Map[T, Int]()

    var partitionSizes: TreeSet[(Int, Double)] = new TreeSet[(Int, Double)]()
    var partitionToSizeMap = new HashMap[Int, Double]
    (0 until numPartitions).foreach(p => {
      partitionSizes += ((p, 0.0d))
      partitionToSizeMap += (p -> 0.0d)
    })
    var heavyKeysToMigrate: Seq[T] = Seq[T]()
    var currentMigration: Double = 0.0d

    def updateBookkeep(k: T, p: Int, newPartitionSize: Double): Unit = {
      explicitHash += (k -> p)
      partitionSizes -= ((p, partitionToSizeMap(p)))
      partitionSizes += ((p, newPartitionSize))
      partitionToSizeMap += (p -> newPartitionSize)
    }

    if (optimizeForMigration) {
      heavyKeysWithFrequencies.foreach { case (k, f) =>
        val oldPartition = getPartition(k)
        val partitionSize = partitionToSizeMap(oldPartition) + heavyKeyToFrequencyMap(k)
        if (partitionSize <= allowedLevel || partitionToSizeMap(oldPartition) == 0.0d) {
          updateBookkeep(k, oldPartition, partitionSize)
        } else {
          heavyKeysToMigrate = heavyKeysToMigrate :+ k
        }
      }

      heavyKeysToMigrate.foreach { k =>
        currentMigration += heavyKeyToFrequencyMap(k)
        val consistentHashPartition: Int = internalPartitioner.getPartition(k)
        val consistentPartitionSize = partitionToSizeMap(consistentHashPartition) +
          heavyKeyToFrequencyMap(k)
        if ((consistentPartitionSize < allowedLevel || partitionToSizeMap(consistentHashPartition) == 0.0d)
          && consistentHashPartition != getPartition(k)) {
          updateBookkeep(k, consistentHashPartition, consistentPartitionSize)
        } else {
          val (p, v) = partitionSizes.min
          val partitionSize = v + heavyKeyToFrequencyMap(k)
          updateBookkeep(k, p, partitionSize)
        }
      }
    } else {
      heavyKeysWithFrequencies.foreach { case (k, f) =>
        val (p, v) = partitionSizes.min
        val freq = heavyKeyToFrequencyMap(k)
        val partitionSize = v + freq
        updateBookkeep(k, p, partitionSize)
        //				explicitHash += (k -> p)
        //				partitionSizes -= ((p, v))
        //				partitionSizes += ((p, partitionSize))
        //				partitionToSizeMap += (p -> partitionSize)
        if (p != getPartition(k)) {
          currentMigration += freq
        }
      }
    }

    // recalculating weighting for the consistent hash partitioner
    val level: Double = PartitioningInfo.newInstance(partitionToSizeMap.toSeq,
      numPartitions, 31).level

    val unnormalizedWeighting: Array[Double] = (0 until numPartitions).map { p =>
      Math.max(0.0d, level - partitionToSizeMap(p))
    }.toArray
    val normalizationFactor: Double = unnormalizedWeighting.sum
    val weighting: Array[Double] = unnormalizedWeighting.map(_ / normalizationFactor)

    // updating consistent hash partitioner
    val consistentHashPartitioner: P = internalPartitioner match {
      case p: Adaptive[P, T] =>
        p.adapt(partitioningInfo, weighting)
      case _ => initializeInternalPartitioner(partitioningInfo, weighting)
    }

    val migrationCostEstimation: Option[(Double, Double)] =
      consistentHashPartitioner.getMigrationCostEstimation match {
        case Some(c) =>
          val numHeavyKeysOut = (heavyKeysMap.keySet -- heavyKeyToFrequencyMap.keySet).count(k =>
            heavyKeysMap(k) != consistentHashPartitioner.getPartition(k))
          val heavyKeyMigration = currentMigration +
            numHeavyKeysOut * heavyKeysWithFrequencies.last._2
          val fractionOfSmallKeys = 1.0d - heavyKeyToFrequencyMap.values.sum
          val smallKeysMigration = fractionOfSmallKeys * c
          Some((heavyKeyMigration, smallKeysMigration))
        case None => None
      }

    new KeyIsolatorPartitioner(
      numPartitions,
      explicitHash,
      consistentHashPartitioner,
      migrationCostEstimation,
      initializeInternalPartitioner)
  }

}

object KeyIsolatorPartitioner {
  var optimizeForMigration: Boolean = true

  var keyExcess: Double = 2.0d
}