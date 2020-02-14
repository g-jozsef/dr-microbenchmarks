package measurements

import java.nio.ByteBuffer

import com.google.common.hash.Hashing
import partitioner.GedikPartitioner.GedikPartitioner
import partitioner.{ConsistentHashPartitioner, HashPartitioner, KeyIsolatorPartitioner, MixedPartitioner, Partitioner, PartitioningInfo, Updateable}
import utils._

import scala.io.Source

/**
  * Simoultaneous measurement of migration cost and partitioning balance on the supplied
  * time-series data
  */
object TimeSeriesStreamingMBMeasurement {

  // example arguments: KeyIsolator 20 100000 20 1.0 10 0.5 timeSeriesData.txt 0.5
  def main(args: Array[String]): Unit = {
    // name of the partitioner to measure
    val partitionerName = args(0)
    // number of partitions of the partitioner
    val numPartitions = args(1).toInt
    // size of a data batch that used for one update of the partitioner
    // represents microbatches is Spark streaming
    val batchSize = args(2).toInt
    // number of batches (= number of consecutive updates)
    val numBatches = args(3).toInt
    // fraction of data to sample from each batch
    val sampleFactor = args(4).toDouble
    // on how many iterations should the whole measurement be averaged
    val iterations = args(5).toInt
    // exponential decay parameter of key histogram history
    val retentionRate = args(6).toDouble
    // size of the sample from each batch
    // only a slice of this size is used from each batch to make data drift between batches more
    // significant
    val sampleSize = (sampleFactor * batchSize).toInt
    // partitioner parameter, currently out of use
    val treeDepthHint = (Math.log10(numPartitions) / Math.log10(2.0d)).toInt + 2
    // operator states are kept for the last `stateWindow` batches
    val stateWindow = 5

    // path to the data
    val dataFile: String = args(7)
    // batches sampled from the data file
    // sampling is done in order to make data drift between batches more significant
    val sample =
    Source.fromFile(dataFile).getLines
      .map(line => line.split("""\|""")(6))
      .flatMap(_.split(","))
      .filter(_ != "-1").take(batchSize * numBatches)
      .grouped(batchSize)
      .map(group => group.take(sampleSize)).toSeq

    // merging two maps by adding corresponding value together
    def mergeMaps(map1: Map[Any, Double], map2: Map[Any, Double]): Map[Any, Double] = {
      var resultMap = map1
      map2.foreach(entry => resultMap.get(entry._1) match {
        case Some(v) => resultMap = resultMap + (entry._1 -> (v + entry._2))
        case None => resultMap = resultMap + entry
      })
      resultMap
    }

    val preKeyDistributions = sample
      .map(group => group.foldLeft(Map[Any, Double]())((map, record) => map.get(record) match {
        case Some(freq) => map + (record -> (freq + 1.0d))
        case None => map + (record -> 1.0d)
      }))
    val preKeyDistributionsWithHistory = preKeyDistributions.scanLeft(Map[Any, Double]())((map1, map2) => mergeMaps(map1, map2)).drop(1)

    // normed key histograms per batch
    val keyDistributionsMap = preKeyDistributions.map(m => m.mapValues(v => v / sampleSize))
    val keyDistributionsWithHistoryMap = preKeyDistributionsWithHistory.zip(1 to numBatches).map({ case (m, i) =>
      val factor = i * sampleSize
      m.mapValues(v => v / factor)
    })
    // retentive key histograms per batch
    // this is a weighted average of the current histogram and the history (exponential decay)
    val retentiveKeyDistributions = keyDistributionsMap.tail.scanLeft(keyDistributionsMap.head)({ case (map1, map2) =>
      mergeMaps(map1.mapValues(v => v * retentionRate), map2.mapValues(v => v * (1.0 - retentionRate)))
    }).map(m => m.toArray.sortBy(-_._2))

    // aggregated unnormed key histograms used as history for key distribution; currently unused
    val keyDistributionsWithHistory = keyDistributionsWithHistoryMap.map(m => m.toArray.sortBy(-_._2))
    // all keys throughout all batches
    val allKeys = preKeyDistributionsWithHistory.last.keySet.toSeq

    // size of state per key and batch
    // we suppose that states are kept for the last stateWindow batches by a stateful operator
    // the size of state is assumed to be linear in the size of a key-group
    val stateSizes = (1 - stateWindow until numBatches - stateWindow + 1 by 1).map(i => keyDistributionsMap.slice(i, i + stateWindow)).map(g =>
      g.foldLeft(Map[Any, Double]())((map1, map2) => mergeMaps(map1, map2)).mapValues(_ / stateWindow))

    /**
      * Create the appropriate partitioner based on the partitioner's name.
      * Valid names are Scan, Redist, Readj, KeyIsolator and Mixed.
      * All of these partitioners are Updateable.
      *
      * @return
      */
    def createNewPartitioner(): Updateable = {
      partitionerName match {
        case "Scan" | "Redist" | "Readj" =>
          // assume linear processing time for key-groups
          val betaS: Double => Double = x => x
          val betaC: Double => Double = x => x
          val thetaS = 0.2
          val thetaC = 0.2
          val thetaN = 0.2
          val utility: (Double, Double) => Double = {
            // optimize for both balance and migration
            case (balancePenalty, migrationPenalty) => balancePenalty + migrationPenalty
          }

          // murmurhash library is used for hashing
          val hash = Hashing.murmur3_32()
          val hashFunc: Any => Int = {
            case (x: String) =>
              hash.hashUnencodedChars(x).asInt()
            case (x: Int) =>
              hash.hashInt(x).asInt()
            case (i: Int, j: Int) =>
              val bytes = ByteBuffer.allocate(8)
              bytes.putInt(0, i)
              bytes.putInt(4, j)
              hash.hashBytes(bytes.array()).asInt()
            case _ =>
              throw new UnsupportedOperationException("Currently we can only hash Int or String.")
          }

          new GedikPartitioner(numPartitions, 100, hashFunc,
            betaS, betaC, thetaS, thetaC, thetaN, utility, partitionerName)
        case "KeyIsolator" =>
          // internal partitioner (used for partition small keys) is ConsistentHashPartitioner
          new KeyIsolatorPartitioner[ConsistentHashPartitioner](numPartitions,
            (pi, weighting) => new ConsistentHashPartitioner(weighting, 100))
        case "Mixed" =>
          // max size of hash table set to be the same as for KIP
          val aMax = 2 * numPartitions + 1
          val thetaMax = args(8).toDouble
          new MixedPartitioner(numPartitions, aMax, thetaMax)
        case _ =>
          throw new RuntimeException(s"Unknown partitioner: `$partitionerName`")
      }
    }

    // migration measurements with linear state
    val migrationMeasurements = new Array[Double](numBatches)

    val migrationMeasurementsWithConstantState = new Array[Double](numBatches)
    val balanceMeasurements = new Array[Double](numBatches)

    for (_ <- 1 to iterations) {
      // generate new random keys for each iteration
      val keyMapping = allKeys.zip(generateKeys(allKeys.size)).toMap
      // partitioner for the previous batch
      var partitioner1: Partitioner = null
      // partitioner for the current batch
      var partitioner2: Partitioner = new HashPartitioner(numPartitions)

      // index of current batch
      var i: Int = 0

      // we measure both migration cost and partitioning balance for each batch
      for (dist <- retentiveKeyDistributions) {
        // replace indices with the newly generated keys
        val retentiveKeyHistogram = dist.map({ case (k, v) => (keyMapping(k.toString), v) }).toSeq
        val keyHistogram = keyDistributionsMap(i).map({ case (k, v) => (keyMapping(k.toString), v) })
        val stateSizeHistogram = stateSizes(i).map({ case (k, v) => (keyMapping(k.toString), v) })
        val keyHistogramWithHistory = keyDistributionsWithHistoryMap(i).map({ case (k, v) => (keyMapping(k.toString), v) })
        val retentiveKeyHistogramMap = retentiveKeyHistogram.toMap
        // set of keys
        val keys = retentiveKeyHistogram.map(_._1)
        // create partitiningInfo
        val partitioningInfo = partitionerName match {
          case "Mixed" =>
            // for Mixed partitioner, a partition histogram is needed
            var retentivePartitionHistogram = (0 until numPartitions).map(p => p -> 0.0d).toMap
            val partit = if (i == 0) createNewPartitioner() else partitioner2
            keys.foreach(k => {
              val part = partit.getPartition(k)
              retentivePartitionHistogram = retentivePartitionHistogram + (part -> (retentivePartitionHistogram(part) + retentiveKeyHistogramMap(k)))
            })
            PartitioningInfo.newInstance(retentiveKeyHistogram, numPartitions, treeDepthHint, partitionHistogram = Some(retentivePartitionHistogram))
          case _ => PartitioningInfo.newInstance(retentiveKeyHistogram.take(2 * numPartitions + 1), numPartitions, treeDepthHint)
        }

        // best balance achievable for any key distribution and partitioner
        val optimalBalance = 1.0d / numPartitions
        println(retentiveKeyHistogram.take(10))
        // update partitioners
        partitioner1 = partitioner2
        partitioner2 = partitioner2 match {
          case p: Updateable =>
            p.update(partitioningInfo).asInstanceOf[Partitioner]
          case _ => createNewPartitioner().update(partitioningInfo).asInstanceOf[Partitioner]
        }

        // measure partitioning balance and migration cost with linear and constant state
        migrationMeasurements(i) += measureExactMigrationCost(partitioner1, partitioner2, stateSizeHistogram)
        migrationMeasurementsWithConstantState(i) += measureExactMigrationCost2(partitioner1, partitioner2, stateSizeHistogram)
        balanceMeasurements(i) += measureBalance(optimalBalance, partitioner1, keyHistogram)
        i = i + 1
      }
    }

    // average measurements and print them
    for (i <- 0 until numBatches) {
      migrationMeasurements(i) /= iterations
      migrationMeasurementsWithConstantState(i) /= iterations
      balanceMeasurements(i) /= iterations
    }
    println(s"###migration (% records) ${migrationMeasurements.mkString("", " ", "")}")
    println(s"###migration (% keys) ${migrationMeasurementsWithConstantState.mkString("", " ", "")}")
    println(s"###relative balance ${balanceMeasurements.mkString("", " ", "")}")
  }

  // create key historam from a list of keys
  def createKeyDistribution(records: Seq[Any]): Array[Double] = {
    records.groupBy(identity).mapValues(_.size).values.toArray.sortBy(-_)
      .map(f => f.toDouble / records.size)
  }

  // generate an array of random string keys (with sequential indices attached to the end)
  def generateKeys(numKeys: Int): Array[Any] = {
    (0 until numKeys).map(k => transformKey(k, k)).toArray
  }

  // generate a random string key with the seed attached to the end
  def transformKey(k: Int, seed: Int): String = StringGenerator.generateRandomString(k) + seed.toString

  // calculate migration cost on a partitioner update assuming linear state
  def measureExactMigrationCost(
                                 oldPartitioner: Partitioner,
                                 newPartitioner: Partitioner,
                                 keyHistogram: Map[Any, Double]): Double = {
    keyHistogram.filterKeys(k => oldPartitioner.getPartition(k) != newPartitioner.getPartition(k)).values.sum
  }

  // calculate migration cost on a partitioner update assuming constant state
  def measureExactMigrationCost2(
                                  oldPartitioner: Partitioner,
                                  newPartitioner: Partitioner,
                                  keyHistogram: Map[Any, Double]): Double = {
    keyHistogram.filterKeys(k => oldPartitioner.getPartition(k) != newPartitioner.getPartition(k)).size.toDouble / keyHistogram.size
  }

  // calculate balance of partitioning relative to the optimal balance
  def measureBalance(
                      optimalBalance: Double,
                      partitioner: Partitioner,
                      keyHistogram: Map[Any, Double]): Double = {
    val partitionHistogram = new Array[Double](partitioner.numPartitions)
    keyHistogram.toSeq.foreach({ case (k, v) =>
      partitionHistogram(partitioner.getPartition(k)) += v
    })
    partitionHistogram.max / optimalBalance
  }

  // normalize a map
  def normalizeMap(map: Map[Any, Double]): Map[Any, Double] = {
    val sum = map.values.sum
    map.mapValues(v => v / sum)
  }
}