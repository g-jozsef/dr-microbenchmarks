package measurements

import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}

import com.google.common.hash.Hashing
import partitioner.GedikPartitioner.GedikPartitioner
import partitioner.Partitioner.PartitionerType
import partitioner.Partitioner.PartitionerType.PartitionerType
import partitioner.{ConsistentHashPartitioner, HashPartitioner, KeyIsolatorPartitioner, MixedPartitioner, Partitioner, PartitioningInfo, Updateable}
import utils._

import scala.io.Source

/**
  * Simoultaneous measurement of migration cost and partitioning balance on the supplied
  * time-series data
  */
object TimeSeriesStreamingMBMeasurement extends PartitionerBenchmarkOptionHandler {
  private val usage: String =
    s"""
       | TimeSeriesStreamingMBMeasurement
       | example parameters: -input timeSeriesData.txt -type KeyIsolator -npart 20 -batchsize 100000 -nbatch 20  -samplefactor 1.0 -iter 10 -retentionrate 0.5 -tmax 0.5
       |
       | ${super.getUsage}
       |
       |  -input: input file path
       |
       |  -batchsize: size of a data batch that used for one update of the partitioner, represents microbatches is Spark streaming
       |  -nbatch: number of batches (= number of consecutive updates)
       |  -samplefactor: fraction of data to sample from each batch
       |  -retentionrate: exponential decay parameter of key histogram history
       |""".stripMargin

  private val options: OptionFactoryMap = super.getOptions ++ Map(
    "-batchsize" -> ('batchSize, (x: String) => x.toInt),
    "-nbatch" -> ('numBatches, (x: String) => x.toInt),
    "-samplefactor" -> ('sampleFactor, (x: String) => x.toDouble),
    "-input" -> ('input, (x: String) => {
      if(!Files.exists(Paths.get(x)))
        throw new IllegalArgumentException(s"Specified file (${x}) does not exist")
      x
    }),
    "-retentionrate" -> ('retentionRate, (x: String) => x.toDouble)
  )

  private val defaultOptions: OptionMap = super.getDefaultOptions ++ Map(
    'numPartitions -> 20,
    'batchSize -> 100000,
    'numBatches -> 20,
    'sampleFactor -> 1.0d,
    'iterations -> 10,
    'retentionRate -> 0.5d,
    'thetaMax -> 0.5d
  )

  override def getUsage: String = usage

  override def getDefaultOptions: OptionMap = defaultOptions

  override def getOptions: Map[String, (Symbol, String => Any)] = options

  type KeyType = String

  def main(args: Array[String]): Unit = {
    readOptions(args)

    val sampleFactor: Double = getOption('sampleFactor)
    val batchSize: Int = getOption('batchSize)
    val numPartitions: Int = getOption('numPartitions)
    val numBatches: Int = getOption('numBatches)
    val input: String = getOption('input)
    val retentionRate: Double = getOption('retentionRate)
    val partitionerType: PartitionerType = getOption('partitionerType)
    val iterations: Int = getOption('iterations)
    val thetaMax: Double = getOption('thetaMax)
    val keyExcess: Int = getOption('keyExcess)


    val sampleSize = (sampleFactor * batchSize).toInt
    // partitioner parameter, currently out of use
    val treeDepthHint = (Math.log10(numPartitions) / Math.log10(2.0d)).toInt + 2
    // operator states are kept for the last `stateWindow` batches
    val stateWindow = 5

    // batches sampled from the data file
    // sampling is done in order to make data drift between batches more significant

    val inputSource = Source.fromFile(input)

    val sample = inputSource.getLines
      .map(line => line.split("""\|""")(6))
      .flatMap(_.split(","))
      .filter(_ != "-1").take(batchSize * numBatches)
      .grouped(batchSize)
      .map(group => group.take(sampleSize)).toSeq

    inputSource.close()

    // merging two maps by adding corresponding value together
    def mergeMaps(map1: Map[KeyType, Double], map2: Map[KeyType, Double]): Map[KeyType, Double] = {
      var resultMap = map1
      map2.foreach(entry => resultMap.get(entry._1) match {
        case Some(v) => resultMap = resultMap + (entry._1 -> (v + entry._2))
        case None => resultMap = resultMap + entry
      })
      resultMap
    }

    val preKeyDistributions = sample
      .map(group => group.foldLeft(Map[KeyType, Double]())((map, record) => map.get(record) match {
        case Some(freq) => map + (record -> (freq + 1.0d))
        case None => map + (record -> 1.0d)
      }))
    val preKeyDistributionsWithHistory = preKeyDistributions.scanLeft(Map[KeyType, Double]())((map1, map2) => mergeMaps(map1, map2)).drop(1)

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
      g.foldLeft(Map[KeyType, Double]())((map1, map2) => mergeMaps(map1, map2)).mapValues(_ / stateWindow))

    // migration measurements with linear state
    val migrationMeasurements = new Array[Double](numBatches)

    val migrationMeasurementsWithConstantState = new Array[Double](numBatches)
    val balanceMeasurements = new Array[Double](numBatches)

    for (_ <- 1 to iterations) {
      // generate new random keys for each iteration
      val keyMapping = allKeys.zip(generateKeys(allKeys.size)).toMap
      // partitioner for the previous batch
      var partitioner1: Partitioner[KeyType] = null
      // partitioner for the current batch
      var partitioner2: Partitioner[KeyType] = new HashPartitioner[KeyType](numPartitions)

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
        val partitioningInfo = partitionerType match {
          case PartitionerType.Mixed =>
            // for Mixed partitioner, a partition histogram is needed
            var retentivePartitionHistogram = (0 until numPartitions).map(p => p -> 0.0d).toMap
            val partit = if (i == 0) Updateable[KeyType](partitionerType, numPartitions, keyExcess, thetaMax) else partitioner2
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
          case p: Updateable[KeyType] =>
            p.update(partitioningInfo).asInstanceOf[Partitioner[KeyType]]
          case _ => Updateable(partitionerType, numPartitions, keyExcess, thetaMax).update(partitioningInfo).asInstanceOf[Partitioner[KeyType]]
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
  def createKeyDistribution(records: Seq[KeyType]): Array[Double] = {
    records.groupBy(identity).mapValues(_.size).values.toArray.sortBy(-_)
      .map(f => f.toDouble / records.size)
  }

  // generate an array of random string keys (with sequential indices attached to the end)
  def generateKeys(numKeys: Int): Array[KeyType] = {
    (0 until numKeys).map(k => transformKey(k, k)).toArray
  }

  // generate a random string key with the seed attached to the end
  def transformKey(k: Int, seed: Int): String = StringGenerator.generateRandomString(k) + seed.toString

  // calculate migration cost on a partitioner update assuming linear state
  def measureExactMigrationCost(
                                 oldPartitioner: Partitioner[KeyType],
                                 newPartitioner: Partitioner[KeyType],
                                 keyHistogram: Map[KeyType, Double]): Double = {
    keyHistogram.filterKeys(k => oldPartitioner.getPartition(k) != newPartitioner.getPartition(k)).values.sum
  }

  // calculate migration cost on a partitioner update assuming constant state
  def measureExactMigrationCost2(
                                  oldPartitioner: Partitioner[KeyType],
                                  newPartitioner: Partitioner[KeyType],
                                  keyHistogram: Map[KeyType, Double]): Double = {
    keyHistogram.filterKeys(k => oldPartitioner.getPartition(k) != newPartitioner.getPartition(k)).size.toDouble / keyHistogram.size
  }

  // calculate balance of partitioning relative to the optimal balance
  def measureBalance(
                      optimalBalance: Double,
                      partitioner: Partitioner[KeyType],
                      keyHistogram: Map[KeyType, Double]): Double = {
    val partitionHistogram = new Array[Double](partitioner.numPartitions)
    keyHistogram.toSeq.foreach({ case (k, v) =>
      partitionHistogram(partitioner.getPartition(k)) += v
    })
    partitionHistogram.max / optimalBalance
  }

  // normalize a map
  def normalizeMap(map: Map[KeyType, Double]): Map[KeyType, Double] = {
    val sum = map.values.sum
    map.mapValues(v => v / sum)
  }
}