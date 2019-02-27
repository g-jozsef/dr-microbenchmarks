package measurements

import java.nio.ByteBuffer

import com.google.common.hash.Hashing
import utils.GedikPartitioner.GedikPartitioner
import utils._

import scala.io.Source

object TimeSeriesStreamingMBMeasurement {

  def main(args: Array[String]): Unit = {
    val partitionerName = args(0)
    val numPartitions = args(1).toInt
    val batchSize = args(2).toInt
    val numBatches = args(3).toInt
    val sampleFactor = args(4).toDouble
    val iterations = args(5).toInt
    val retentionRate = args(6).toDouble
    val sampleSize = (sampleFactor * batchSize).toInt
    val treeDepthHint = (Math.log10(numPartitions) / Math.log10(2.0d)).toInt + 2
    val stateWindow = 5

    val dataFile: String = """C:\Users\szape\Work\Projects\ER-DR\data\timeseries_ordered.txt"""
    val sample =
      Source.fromFile(dataFile).getLines
        .map(line => line.split("""\|""")(6))
        .flatMap(_.split(","))
        .filter(_ != "-1").take(batchSize * numBatches)
        .grouped(batchSize)
        .map(group => group.take(sampleSize)).toSeq

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
    val keyDistributionsMap = preKeyDistributions.map(m => m.mapValues(v => v / sampleSize))


    val keyDistributionsWithHistoryMap = preKeyDistributionsWithHistory.zip(1 to numBatches).map({ case (m, i) =>
      val factor = i * sampleSize
      m.mapValues(v => v / factor)
    })
    val retentiveKeyDistributions = keyDistributionsMap.tail.scanLeft(keyDistributionsMap.head)({ case (map1, map2) =>
      mergeMaps(map1.mapValues(v => v * retentionRate), map2.mapValues(v => v * (1.0 - retentionRate)))
    }).map(m => m.toArray.sortBy(-_._2))

    val keyDistributionsWithHistory = keyDistributionsWithHistoryMap.map(m => m.toArray.sortBy(-_._2))
    val allKeys = preKeyDistributionsWithHistory.last.keySet.toSeq

    val stateSizes = (1 - stateWindow until numBatches - stateWindow + 1 by 1).map(i => keyDistributionsMap.slice(i, i + stateWindow)).map(g =>
      g.foldLeft(Map[Any, Double]())((map1, map2) => mergeMaps(map1, map2)).mapValues(_ / stateWindow))

    def createNewPartitioner(): Updateable = {
      partitionerName match {
        case "Scan" | "Redist" | "Readj" =>
          val betaS: Double => Double = x => x
          val betaC: Double => Double = x => x
          val thetaS = 0.2
          val thetaC = 0.2
          val thetaN = 0.2
          val utility: (Double, Double) => Double = {
            case (balancePenalty, migrationPenalty) => balancePenalty + migrationPenalty
          }

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
          new KeyIsolatorPartitioner[ConsistentHashPartitioner](numPartitions,
            (pi, weighting) => new ConsistentHashPartitioner(weighting, 100))
        case "Mixed" =>
          val aMax = 2 * numPartitions + 1
          val thetaMax = args(7).toDouble
          new MixedPartitioner(numPartitions, aMax, thetaMax)
        case _ =>
          throw new RuntimeException(s"Unknown partitioner: `$partitionerName`")
      }
    }

    val migrationMeasurements = new Array[Double](numBatches)
    val migrationMeasurementsWithConstantState = new Array[Double](numBatches)
    val balanceMeasurements = new Array[Double](numBatches)

    for (_ <- 1 to iterations) {
      val keyMapping = allKeys.zip(generateKeys(allKeys.size)).toMap
      var partitioner1: Partitioner = null
      var partitioner2: Partitioner = new HashPartitioner(numPartitions)

      var i: Int = 0

      for (dist <- retentiveKeyDistributions) {
        val retentiveKeyHistogram = dist.map({ case (k, v) => (keyMapping(k.toString), v) }).toSeq
        val keyHistogram = keyDistributionsMap(i).map({ case (k, v) => (keyMapping(k.toString), v) })
        val stateSizeHistogram = stateSizes(i).map({ case (k, v) => (keyMapping(k.toString), v) })
        val keyHistogramWithHistory = keyDistributionsWithHistoryMap(i).map({ case (k, v) => (keyMapping(k.toString), v) })
        val retentiveKeyHistogramMap = retentiveKeyHistogram.toMap
        val keys = retentiveKeyHistogram.map(_._1)
        val partitioningInfo = partitionerName match {
          case "Mixed" =>
            var retentivePartitionHistogram = (0 until numPartitions).map(p => p -> 0.0d).toMap
            val partit = if (i == 0) createNewPartitioner() else partitioner2
            keys.foreach(k => {
              val part = partit.getPartition(k)
              retentivePartitionHistogram = retentivePartitionHistogram + (part -> (retentivePartitionHistogram(part) + retentiveKeyHistogramMap(k)))
            })
            PartitioningInfo.newInstance(retentiveKeyHistogram, numPartitions, treeDepthHint, partitionHistogram = Some(retentivePartitionHistogram))
          case _ => PartitioningInfo.newInstance(retentiveKeyHistogram.take(2 * numPartitions + 1), numPartitions, treeDepthHint)
        }

        val optimalBalance = 1.0d / numPartitions
        println(retentiveKeyHistogram.take(10))
        partitioner1 = partitioner2
        partitioner2 = partitionerName match {
          case "Hash" => partitioner2
          case _ => partitioner2 match {
            case p: Updateable =>
              p.update(partitioningInfo).asInstanceOf[Partitioner]
            case _ => createNewPartitioner().update(partitioningInfo).asInstanceOf[Partitioner]
          }
        }
        migrationMeasurements(i) += measureExactMigrationCost(partitioner1, partitioner2, stateSizeHistogram)
        migrationMeasurementsWithConstantState(i) += measureExactMigrationCost2(partitioner1, partitioner2, stateSizeHistogram)
        balanceMeasurements(i) += measureBalance(optimalBalance, partitioner1, keyHistogram)
        i = i + 1
      }
    }

    for (i <- 0 until numBatches) {
      migrationMeasurements(i) /= iterations
      migrationMeasurementsWithConstantState(i) /= iterations
      balanceMeasurements(i) /= iterations
    }
    println(s"###migration (% records) ${migrationMeasurements.mkString("", " ", "")}")
    println(s"###migration (% keys) ${migrationMeasurementsWithConstantState.mkString("", " ", "")}")
    println(s"###relative balance ${balanceMeasurements.mkString("", " ", "")}")
  }

  def createKeyDistribution(records: Seq[Any]): Array[Double] = {
    records.groupBy(identity).mapValues(_.size).values.toArray.sortBy(-_)
      .map(f => f.toDouble / records.size)
  }

  def generateKeys(numKeys: Int): Array[Any] = {
    (0 until numKeys).map(k => transformKey(k, k)).toArray
  }

  def transformKey(k: Int, seed: Int): String = StringGenerator.generateRandomString(k) + seed.toString

  def measureExactMigrationCost(
    oldPartitioner: Partitioner,
    newPartitioner: Partitioner,
    keyHistogram: Map[Any, Double]): Double = {
    keyHistogram.filterKeys(k => oldPartitioner.getPartition(k) != newPartitioner.getPartition(k)).values.sum
  }

  def measureExactMigrationCost2(
    oldPartitioner: Partitioner,
    newPartitioner: Partitioner,
    keyHistogram: Map[Any, Double]): Double = {
    keyHistogram.filterKeys(k => oldPartitioner.getPartition(k) != newPartitioner.getPartition(k)).size.toDouble / keyHistogram.size
  }

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

  def normalizeMap(map: Map[Any, Double]): Map[Any, Double] = {
    val sum = map.values.sum
    map.mapValues(v => v / sum)
  }
}