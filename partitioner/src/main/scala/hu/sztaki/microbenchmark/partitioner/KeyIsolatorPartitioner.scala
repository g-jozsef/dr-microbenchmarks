package hu.sztaki.microbenchmark.partitioner

import scala.collection.mutable

class KeyIsolatorPartitioner[P <: Adaptive[P, T] with MigrationCostEstimator, T](
    override val numPartitions: Int,
    val initializeInternalPartitioner: (PartitioningInfo[T], Array[Double]) => P,
    val optimizeForMigration: Boolean = true,
    val keyExcess: Double = 2.0d)
  extends Updateable[T] with MigrationCostEstimator {

  protected var internalPartitioner: Partitioner[T] = new HashPartitioner[T](numPartitions)
  var heavyKeysMap: mutable.Map[T, Int] = mutable.Map.empty[T, Int]
  protected var migrationCostEstimation: Option[(Double, Double)] = None

  override def getPartition(key: T): Int =
    if(heavyKeysMap.contains(key)) heavyKeysMap(key) else internalPartitioner.getPartition(key)

  override def getMigrationCostEstimation: Option[Double] =
    migrationCostEstimation.map({ case (hm, sm) => hm + sm })

  def getInternalMigrationCostEstimation: Option[Double] =
    migrationCostEstimation.map({ case (_, sm) => sm })

  override def update(partitioningInfo: PartitioningInfo[T]): Updateable[T] = {
    val allowedBalanceError = 0.001d
    val numHeavyKeys = Math.min((numPartitions * keyExcess).round.toInt, partitioningInfo.heavyKeys.size)
    // ordered by frequency
    val heavyKeysWithFrequencies = partitioningInfo.heavyKeys.take(numHeavyKeys)
    val heavyKeyToFrequencyMap: Map[T, Double] = heavyKeysWithFrequencies.toMap
    val allowedLevel = Math.max(heavyKeysWithFrequencies.head._2, 1.0d / numPartitions) + allowedBalanceError
    val explicitHash : mutable.Map[T, Int] = mutable.Map.empty[T, Int]

    val partitionSizes: mutable.Map[Int, Double] = mutable.Map.empty[Int, Double]
    (0 until numPartitions).foreach(partition => {
      partitionSizes(partition) = 0.0d
    })

    var currentMigration: Double = 0.0d

    def updateBookkeeping(key: T, partition: Int, newPartitionSize: Double): Unit = {
      explicitHash(key) = partition
      partitionSizes(partition) = newPartitionSize
    }

    if (optimizeForMigration) {
      heavyKeysWithFrequencies.foreach { case (key, frequency)=>
        val oldPartition: Int = getPartition(key)
        val partitionSize: Double = partitionSizes(oldPartition) + frequency

        if (partitionSize <= allowedLevel || partitionSizes(oldPartition) == 0.0d) {
          //Migration not needed
          updateBookkeeping(key, oldPartition, partitionSize)
        } else {
          //Migration required

          currentMigration += frequency

          val consistentHashPartition: Int = internalPartitioner.getPartition(key)
          val consistentPartitionSize: Double = partitionSizes(consistentHashPartition) + frequency

          //todo consistentPartitionSize < allowedLevel ez volt itt, de why?
          if ((consistentPartitionSize <= allowedLevel || partitionSizes(consistentHashPartition) == 0.0d)
            && consistentHashPartition != getPartition(key)) {

            updateBookkeeping(key, consistentHashPartition, consistentPartitionSize)
          } else {
            val (partition: Int, currentValue: Double) = partitionSizes.minBy(x => x._2 -> x._1)
            updateBookkeeping(key, partition, currentValue + frequency)
          }
        }
      }
    } else {
      heavyKeysWithFrequencies.foreach { case (key, frequency) =>
        val (partition: Int, currentValue: Double) = partitionSizes.minBy(x => x._2 -> x._1)

        updateBookkeeping(key, partition, currentValue + frequency)

        if (partition != getPartition(key)) {
          currentMigration += frequency
        }
      }
    }

    // recalculating weighting for the consistent hash partitioner
    val level: Double = PartitioningInfo(partitionSizes.toSeq,
      numPartitions, 31).level

    val unnormalizedWeighting: Array[Double] = (0 until numPartitions).map { p =>
      Math.max(0.0d, level - partitionSizes(p))
    }.toArray
    val normalizationFactor: Double = unnormalizedWeighting.sum
    val weighting: Array[Double] = unnormalizedWeighting.map(_ / normalizationFactor)

    // updating consistent hash partitioner
    val consistentHashPartitioner: P = internalPartitioner match {
      case p: Adaptive[P, T] =>
        p.adapt(partitioningInfo, weighting)
      case _ => initializeInternalPartitioner(partitioningInfo, weighting)
    }

    val newMigrationCostEstimation: Option[(Double, Double)] =
      consistentHashPartitioner.getMigrationCostEstimation match {
        case Some(c) =>
          val numHeavyKeysOut = (heavyKeysMap.keySet -- heavyKeyToFrequencyMap.keySet).count(key =>
            heavyKeysMap(key) != consistentHashPartitioner.getPartition(key))
          val heavyKeyMigration = currentMigration + numHeavyKeysOut * heavyKeysWithFrequencies.last._2
          val fractionOfSmallKeys = 1.0d - heavyKeyToFrequencyMap.values.sum
          val smallKeysMigration = fractionOfSmallKeys * c
          Some((heavyKeyMigration, smallKeysMigration))
        case None => None
      }

    heavyKeysMap = explicitHash
    internalPartitioner = consistentHashPartitioner
    migrationCostEstimation = newMigrationCostEstimation

    this
  }
}