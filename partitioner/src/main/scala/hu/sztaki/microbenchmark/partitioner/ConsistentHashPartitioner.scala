package hu.sztaki.microbenchmark.partitioner

import scala.util.hashing.MurmurHash3

class ConsistentHashPartitioner[T] private(
  override val numPartitions: Int,
  val replicationFactor: Int,
  val weighting: Array[Double],
  val hashRing: Array[Int],
  val migrationCostEstimation: Option[Double],
  hashFunction: Any => Double) extends Adaptive[ConsistentHashPartitioner[T], T] with MigrationCostEstimator {

  // TODO construct with numReplicas instead of replicationFactor
  private def this(
    numPartitions: Int,
    replicationFactor: Int,
    weighting: Array[Double],
    initialPoints: Array[Int],
    hashFunction: Any => Double) {
    this(numPartitions,
      replicationFactor,
      weighting,
      ConsistentHashPartitioner.createInitialHashRing(numPartitions, initialPoints),
      None,
      hashFunction)
  }

  def this(
    weighting: Array[Double],
    replicationFactor: Int = 100,
    hashFunction: Any => Double = key =>
      (MurmurHash3.stringHash((key.hashCode + 123456791).toString).toDouble / Int.MaxValue + 1) / 2) {
    this(weighting.length,
      replicationFactor,
      weighting,
      ConsistentHashPartitioner.createInitialPoints(weighting.length * replicationFactor, weighting),
      hashFunction)
  }

  private val numPackages = numPartitions * replicationFactor

  override def getPartition(key: T): Int = {
    hashRing(Math.floor(hashFunction(key) * numPackages).toInt % numPackages)
  }

  override def getMigrationCostEstimation: Option[Double] = migrationCostEstimation

  override def adapt(
    partitioningInfo: PartitioningInfo[T],
    newWeighting: Array[Double]): ConsistentHashPartitioner[T] = {
    val newHashRing: Array[Int] = new Array[Int](numPackages)

    val transfer: Array[Int] = weighting.zip(newWeighting).map(p => p._2 - p._1)
      .scan(0.0d)(_ + _)
      .map(d => Math.round(d * numPackages).toInt) // assert last = 0
      .sliding(2).map(p => p(1) - p(0)).toArray
    val migrationCostEstimation = transfer.filter(t => t > 0).sum.toDouble / numPackages
    var transferTo: Int = 0
    //transfer.find(part => transfer(part) > 0).get
    while (transferTo < transfer.length && transfer(transferTo) <= 0) {
      transferTo += 1
    }
    for (replica <- 0 until numPackages) {
      val oldPartition = hashRing(replica)
      if (transferTo < transfer.length && transfer(oldPartition) < 0) {
        newHashRing(replica) = transferTo
        // Updating transfer
        transfer(oldPartition) += 1
        transfer(transferTo) -= 1
        // Updating transferTo
        while (transferTo < transfer.length && transfer(transferTo) <= 0) {
          transferTo += 1
        }
      } else {
        newHashRing(replica) = oldPartition
      }
    }

    //		val (transferSource, transferDestination) = partitionsToPackages.partition { case (k, _) => transfer(k) < 0 }
    //		var packagePool: Seq[Int] = transferSource.flatMap { case (k, v) =>
    //			partitionsToPackages += (k -> v.dropRight(-transfer(k)))
    //			 v.takeRight(-transfer(k))
    //		}.toSeq
    //
    //		transferDestination.foreach { case (k, v) =>
    //			val acquired = packagePool.take(transfer(k))
    //			packagePool = packagePool.drop(transfer(k))
    //			partitionsToPackages += (k -> (v ++ acquired))
    //			acquired.foreach(a => hashRing(a) = k)
    //		}
    //		var hashRing = this.hashRing ++ transferDestination.flatMap { case (k, v) =>
    //			packagePool.take(transfer(k)).map(a => k -> a)}

    new ConsistentHashPartitioner(
      numPartitions,
      replicationFactor,
      newWeighting,
      newHashRing,
      Some(migrationCostEstimation),
      hashFunction)
  }

  // for testing
  def _getPartition(hash: Double): Int = {
    hashRing(Math.floor(hash * numPackages).toInt % numPackages)
  }

  // for testing
  def printHashRing(): Unit = {
    println("Hash ring: " + hashRing.mkString("[", ", ", "]"))
  }

}

object ConsistentHashPartitioner {

  def createInitialPoints(numPackages: Int, weighting: Array[Double]): Array[Int] = {
    weighting.scan(0.0d)(_ + _).map(w => Math.rint(w * numPackages).toInt)
    // TODO add normalization correction
    // tmpAggregate
    //	private val initialPoints = tmpAggregate.map(w => Math.rint(w / tmpAggregate.last * numPackages).toInt)
    //	for (p: Int <- 1 to numPartitions; b: Int <- initialPoints(p - 1) until initialPoints(p)) {
    //		hashRing += (b.toDouble / (numPartitions * replicationFactor) -> (p - 1))
    //	}
  }

  def createInitialHashRing(numPartitions: Int, initialPoints: Array[Int]): Array[Int] = {
    val hashRing: Array[Int] = new Array[Int](initialPoints.last)
    (0 until numPartitions).foreach { part =>
      (initialPoints(part) until initialPoints(part + 1)).foreach { host =>
        hashRing(host) = part
      }
    }
    hashRing
    //		(TreeMap[Int, Int]() /: (0 until numPartitions)) { (map, part) =>
    //			map ++ (initialPoints(part) until initialPoints(part + 1)).map { b => b -> part }
    //		}
  }
}