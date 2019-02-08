package utils

import utils.AdvancedBinarySearch._
import utils.MixedPartitioner._

class MixedPartitioner(
  val numPartitions: Int,
  val aMax: Int,
  val thetaMax: Double,
  val routingTable: Map[Any, Int],
  val consistentHasher: ConsistentHashPartitioner) extends Updateable[MixedPartitioner] {

  //	def this(numPartitions: Int, aMax: Int, thetaMax: Double,
  //		consistentHasher: ConsistentHashPartitioner) = {
  //		this(numPartitions, aMax, thetaMax, Map[Any, Int](),
  //			consistentHasher, createPartitionsToKeysMap(consistentHasher))
  //	}

  def this(numPartitions: Int, aMax: Int, thetaMax: Double) = {
    this(numPartitions, aMax, thetaMax, Map[Any, Int](),
      new ConsistentHashPartitioner(
        Array.fill[Double](numPartitions)(1.0d / numPartitions),
        replicationFactor))
  }

  override def getPartition(key: Any): Int = {
    routingTable.get(key) match {
      case Some(part) => part
      case None => consistentHasher.getPartition(key)
    }
  }

  override def update(partitioningInfo: PartitioningInfo): MixedPartitioner = {

    // Algorithm
    val keysToFrequencies: Map[Any, Double] = partitioningInfo.heavyKeys.toMap[Any, Double]
    var newRoutingTable: Map[Any, Int] = null
    var n = 0
    val maxLoad = (thetaMax + 1.0d) / numPartitions
    val initialLoads = partitioningInfo.partitionHistogram.getOrElse(
      throw new RuntimeException("Partition histogram is missing!"))

    def createPartitionsToKeysMap(): Map[Int, Vector[Any]] = {
      var partitionsToKeysMap = Map[Int, Vector[Any]]()
      for (p <- 0 until numPartitions) {
        partitionsToKeysMap = partitionsToKeysMap + (p -> Vector[Any]())
      }

      for (key <- partitioningInfo.heavyKeys.map(_._1)) {
        val part = getPartition(key)
        partitionsToKeysMap = partitionsToKeysMap + (part -> (partitionsToKeysMap(part) :+ key))
      }
      partitionsToKeysMap
    }

    val initialPartitionsToKeysMap = createPartitionsToKeysMap()
    var heavyKeys = routingTable.keys.toVector.sortBy(key => keysToFrequencies(key))

    def memory(key: Any): Double = {
      keysToFrequencies(key)
    }

    var aIterations = 0
    do {
      // phase I
      // maybe optimize???
      newRoutingTable = routingTable
      var loads = initialLoads
      var partitionsToKeysMap = initialPartitionsToKeysMap
      val movedBackKeys = heavyKeys.take(n)
      newRoutingTable = newRoutingTable -- movedBackKeys
      movedBackKeys.foreach(k => {
        val oldPartition = getPartition(k)
        val newPartition = consistentHasher.getPartition(k)
        val freq = keysToFrequencies(k)
        loads = loads + (oldPartition -> (loads(oldPartition) - freq))
        loads = loads + (newPartition -> (loads(newPartition) + freq))
        partitionsToKeysMap = partitionsToKeysMap +
          (oldPartition -> partitionsToKeysMap(oldPartition).filterNot(k.==))
        partitionsToKeysMap = partitionsToKeysMap +
          (newPartition -> (partitionsToKeysMap(newPartition) :+ k).sortBy(x => -keysToFrequencies(x)))
      })

      // phase II
      // works under the assumption that beta > 1 and c(k) ~ S(k)
      var keysToMigrate: Seq[Any] = Seq[Any]()

      for (part <- 0 until numPartitions) {
        var keys = partitionsToKeysMap(part)
        var minusFreq = 0.0d

        while (loads(part) - minusFreq > maxLoad) {
          val key = keys.head
          keysToMigrate = keysToMigrate :+ key
          keys = keys.tail
          minusFreq = minusFreq + keysToFrequencies(key)
        }
        loads = loads + (part -> (loads(part) - minusFreq))
        partitionsToKeysMap = partitionsToKeysMap + (part -> keys)
      }

      def adjust(key: Any, part: Int): Boolean = {
        val freq = keysToFrequencies(key)
        val load = loads(part)
        if (load + freq <= maxLoad) {
          if (consistentHasher.getPartition(key) != part) {
            newRoutingTable = newRoutingTable + (key -> part)
          }
          keysToMigrate = keysToMigrate.tail
          partitionsToKeysMap = partitionsToKeysMap +
            (part -> (partitionsToKeysMap(part) :+ key).sortBy(k => -keysToFrequencies(k)))
          loads = loads + (part -> (load + freq))
          true
        } else {
          val excess = load + freq - maxLoad
          var sum = 0.0d
          val keys = partitionsToKeysMap(part)
          val start = binarySearch[Any](keys, key, (k: Any) => -keysToFrequencies(k))
          var end = start
          // TODO: test
          while (sum < excess && end < keys.length) {
            sum += keysToFrequencies(keys(end))
            end += 1
          }
          if (sum >= excess) {
            // updating bookeep
            val keysOut = keys.slice(start, end)
            newRoutingTable -- keysOut
            if (consistentHasher.getPartition(key) != part) {
              newRoutingTable = newRoutingTable + (key -> part)
            }
            keysToMigrate = keysToMigrate.tail ++ keysOut
            keysToMigrate = keysToMigrate.sortBy(k => -keysToFrequencies(k))
            partitionsToKeysMap =
              partitionsToKeysMap + (part -> ((keys.take(start) :+ key) ++ keys.drop(end)))
            loads = loads + (part -> (load + freq - sum))
            true
          } else {
            false
          }
        }
      }

      // phase III
      var thetaIterations = 0
      while (keysToMigrate.nonEmpty) {
        val key = keysToMigrate.head
        // TODO: handle case when key cannot be placed anywhere
        // TODO: store partitions in a sorted structure
        var sortedPartitions = (0 until numPartitions).sortBy(p => loads(p)).iterator
        var part = sortedPartitions.next()
        while (!adjust(key, part) && sortedPartitions.hasNext) {
          part = sortedPartitions.next()
        }
        // signal failure here
        if (!sortedPartitions.hasNext && keysToMigrate.nonEmpty && key == keysToMigrate.head) {
          throw new RuntimeException("Failed to migrate heavy keys. Load balance constraint cannot be met.")
        } else {
        }
        thetaIterations += 1
        if (thetaIterations >= maxThetaIterations) {
          throw new RuntimeException("Exceeded allowed number of iterations. Load balance constraint cannot be met.")
        }
      }
      n = newRoutingTable.size - aMax
      aIterations += 1
      if (aIterations >= maxAIterations) {
        throw new RuntimeException("Exceeded allowed number of iterations. Routing table size constraint cannot be met.")
      }
    } while (n > 0)

    new MixedPartitioner(numPartitions, aMax, thetaMax, newRoutingTable, consistentHasher)
  }
}

object MixedPartitioner {
  val replicationFactor: Int = 100
  val beta = 1.5
  val maxThetaIterations = 1000
  val maxAIterations = 1000

  //	def createPartitionsToKeysMap(consistentHasher: ConsistentHashPartitioner): Unit = {
  //		val numPartitions = consistentHasher.numPartitions
  //
  //	}
}