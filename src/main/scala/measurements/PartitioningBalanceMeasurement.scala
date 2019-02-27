package measurements

import java.nio.ByteBuffer

import com.google.common.hash.Hashing
import utils.GedikPartitioner.GedikPartitioner
import utils._

// Figure 5, Figure 7
object PartitioningBalanceMeasurement {

  def main(args: Array[String]): Unit = {
    val partitionerName = args(0)
    val numPartitions = args(1).toInt
    val numKeys = args(2).toInt
    val iterations = args(3).toInt
    val cutdown = args(4).toDouble
    val exponent = args(5).toDouble
    val shift = args(8).toDouble
    val keyExcess = 2

    val treeDepthHint = (Math.log10(1000) / Math.log10(2.0d)).toInt + 2
    val keys = new Array[Any](numKeys)

    def createNewPartitioner(keyHistogram: Array[Double], numPartitions: Int): Partitioner = {
      partitionerName match {
        case "Hash" =>
          new HashPartitioner(numPartitions)
        case _ =>
          val weights = keys.zip(keyHistogram)
          val partitioningInfo = PartitioningInfo.newInstance(weights.take(keyExcess * numPartitions + 1), numPartitions, treeDepthHint)
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
                betaS, betaC, thetaS, thetaC, thetaN, utility, partitionerName).update(partitioningInfo)
            case "KeyIsolator" =>
              new KeyIsolatorPartitioner[ConsistentHashPartitioner](numPartitions,
                (pi, weighting) => new ConsistentHashPartitioner(weighting, 100)).update(partitioningInfo)
            case "Mixed" =>
              val aMax = keyExcess * numPartitions + 1
              val thetaMax = args(11).toDouble
              val partitioner = new MixedPartitioner(numPartitions, aMax, thetaMax)
              var partitionHistogram = (0 until numPartitions).map(p => p -> 0.0d).toMap
              (0 until numKeys).foreach(k => {
                val part = partitioner.getPartition(keys(k))
                partitionHistogram = partitionHistogram + (part -> (partitionHistogram(part) + keyHistogram(k)))
              })
              val partitioningInfo2 = PartitioningInfo.newInstance(weights, numPartitions, treeDepthHint,
                partitionHistogram = Some(partitionHistogram))
              partitioner.update(partitioningInfo2)
            case _ =>
              throw new RuntimeException(s"Unknown partitioner: `$partitionerName`")
          }
      }
    }

    def measureBalance(keyHistogram: Array[Double], numPartitions: Int): Unit = {
      val numKeys = keyHistogram.length
      val absoluteBalance = 1.0d / numPartitions
      val distributionBalance = Math.max(absoluteBalance, keyHistogram.head)

      val balanceMeasurements = new Array[Double](iterations)
      for (i <- 0 until iterations) {
        val partitionHistogram = Array.fill[Double](numPartitions)(0.0d)

        (0 until numKeys).foreach(k => keys(k) = transformKey(k, k))

        var partitioner: Partitioner = createNewPartitioner(keyHistogram, numPartitions)
        (0 until numKeys).foreach(k => {
          partitionHistogram(partitioner.getPartition(keys(k))) += keyHistogram(k)
        })

        val heavyKeys = keys.take(keyExcess * numPartitions)
        var heavyKeysHistogram = Array.fill[List[Double]](numPartitions)(List.empty[Double])
        partitioner.asInstanceOf[KeyIsolatorPartitioner[ConsistentHashPartitioner]]
          .heavyKeysMap.foreach({ case (k, p) => heavyKeysHistogram(p) :+= keyHistogram(heavyKeys.indexOf(k)) })
        heavyKeysHistogram = heavyKeysHistogram.map(l => l.sortBy(-_))

        val sorted = partitionHistogram.zip(heavyKeysHistogram).sortBy(-_._2.sum)
        val s2 = sorted.map(_._2)
        println(sorted.map(_._1).mkString(", "))

        val m: Int = heavyKeysHistogram.map(_.size).max
        for (i <- 1 to m) {
          println(s2.map(l => if (l.size >= i) l(i - 1) else 0.0d).mkString(", "))
        }
        val maxPartitionSize = partitionHistogram.max
        balanceMeasurements(i) = maxPartitionSize
      }
      val drop = (iterations * cutdown).toInt
      val avg = Mean(balanceMeasurements.sorted.drop(drop).dropRight(drop))
      val metric = avg
      print(s"$metric ")
    }

    measureBalance(Distribution.zeta(exponent, shift, numKeys).probabilities, numPartitions)
  }

  def transformKey(k: Int, seed: Int): String = StringGenerator.generateRandomString(k) + seed.toString
}