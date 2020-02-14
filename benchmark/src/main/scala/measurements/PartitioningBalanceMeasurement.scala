package measurements

import java.nio.ByteBuffer

import com.google.common.hash.Hashing
import partitioner.GedikPartitioner.GedikPartitioner
import partitioner.{ConsistentHashPartitioner, HashPartitioner, KeyIsolatorPartitioner, MixedPartitioner, Partitioner, PartitioningInfo}
import utils._

/**
  * Microbenchmark for measuring partitioning balance of different partitioners
  */
// Figure 5, Figure 7
object PartitioningBalanceMeasurement {

  // example arguments: KeyIsolator 50 1000 10 0.1 1.5 10 0.1
  def main(args: Array[String]): Unit = {
    // name of the partitioner to measure
    val partitionerName = args(0)
    // number of partitions of the partitioner
    val numPartitions = args(1).toInt
    // total number of keys in the hypothetic data
    val numKeys = args(2).toInt
    // on how many iterations should the whole measurement be averaged
    val iterations = args(3).toInt
    // measurement cutdown parameter, tells what percentage of measurements should be treated as
    // outliers
    val cutdown = args(4).toDouble
    // exponent of key-distribution
    val exponent = args(5).toDouble
    // shift parameter of key-distribution
    val shift = args(6).toDouble
    // KeyIsolatorPartitioner (KIP) parameter, tells how many keys should be regarded as `heavy`
    // number of heavy keys = keyExcess * numPartitions
    val keyExcess = 2

    // partitioner parameter, currently out of use
    val treeDepthHint = (Math.log10(1000) / Math.log10(2.0d)).toInt + 2
    // array of keys
    val keys = new Array[Any](numKeys)

    /**
      * Create the appropriate partitioner based on the partitioner's name.
      * Valid names are Hash, Scan, Redist, Readj, KeyIsolator and Mixed.
      * Updateable partitioners will be updated with the key-histogram.
      *
      * @param keyHistogram
      * @param numPartitions
      * @return
      */
    def createNewPartitioner(keyHistogram: Array[Double], numPartitions: Int): Partitioner = {
      partitionerName match {
        case "Hash" =>
          new HashPartitioner(numPartitions)
        case _ =>
          // keys with frequencies
          val weights = keys.zip(keyHistogram)
          // informations needed to construct the partitioner
          val partitioningInfo = PartitioningInfo.newInstance(weights.take(keyExcess * numPartitions + 1), numPartitions, treeDepthHint)
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

              // create Gedik partitioner and update it
              new GedikPartitioner(numPartitions, 100, hashFunc,
                betaS, betaC, thetaS, thetaC, thetaN, utility, partitionerName).update(partitioningInfo)
            case "KeyIsolator" =>
              // create KeyIsolatorPartitioner (KIP) and update it
              // internal partitioner (used for partition small keys) is ConsistentHashPartitioner
              new KeyIsolatorPartitioner[ConsistentHashPartitioner](numPartitions,
                (pi, weighting) => new ConsistentHashPartitioner(weighting, 100)).update(partitioningInfo)
            case "Mixed" =>
              // max size of hash table set to be the same as for KIP
              val aMax = keyExcess * numPartitions + 1
              val thetaMax = args(7).toDouble
              // create Mixed partitioner
              val partitioner = new MixedPartitioner(numPartitions, aMax, thetaMax)
              // calculate the partition histogram
              // we need to know the partition histogram for the update
              var partitionHistogram = (0 until numPartitions).map(p => p -> 0.0d).toMap
              (0 until numKeys).foreach(k => {
                val part = partitioner.getPartition(keys(k))
                partitionHistogram = partitionHistogram + (part -> (partitionHistogram(part) + keyHistogram(k)))
              })
              // Mixed partitioner needs a different partitioning info
              val partitioningInfo2 = PartitioningInfo.newInstance(weights, numPartitions, treeDepthHint,
                partitionHistogram = Some(partitionHistogram))
              // update Mixed partitioner
              partitioner.update(partitioningInfo2)
            case _ =>
              throw new RuntimeException(s"Unknown partitioner: `$partitionerName`")
          }
      }
    }

    /**
      * The measurement code. Creates the partitioner and measures partitioning balance under the
      * assumption that the key-distribution is static in time. Results are averaged through many
      * iterations.
      *
      * @param keyHistogram
      * @param numPartitions
      */
    def measureBalance(keyHistogram: Array[Double], numPartitions: Int): Unit = {
      val numKeys = keyHistogram.length
      // the best balance possible without knowing the key histogram (use this as a baseline)
      val absoluteBalance = 1.0d / numPartitions
      // the best balance possible with this key histogram (use this as a baseline)
      val distributionBalance = Math.max(absoluteBalance, keyHistogram.head)

      val balanceMeasurements = new Array[Double](iterations)
      for (i <- 0 until iterations) {
        println(s"\nRunning iteration $i...")
        val partitionHistogram = Array.fill[Double](numPartitions)(0.0d)

        // create keys; new random keys must be created for every measurement to average out
        // the effects of hashing
        (0 until numKeys).foreach(k => keys(k) = transformKey(k, k))

        // create the partitioner
        val partitioner: Partitioner = createNewPartitioner(keyHistogram, numPartitions)
        // calculate the partition histogram
        (0 until numKeys).foreach(k => {
          partitionHistogram(partitioner.getPartition(keys(k))) += keyHistogram(k)
        })

        // this block of code is optional; it prints a sorted list of heavy keys for each
        // partition; good for analyzing heavy-key balance and lightweight-key balance separately
        {
          val heavyKeys = keys.take(keyExcess * numPartitions)
          println(s"Heavy keys: [${heavyKeys.mkString(", ")}]")
          var heavyKeysHistogram = Array.fill[List[Double]](numPartitions)(List.empty[Double])
          partitioner.asInstanceOf[KeyIsolatorPartitioner[ConsistentHashPartitioner]]
            .heavyKeysMap.foreach({ case (k, p) => heavyKeysHistogram(p) :+= keyHistogram(heavyKeys.indexOf(k)) })
          heavyKeysHistogram = heavyKeysHistogram.map(l => l.sortBy(-_))
          val sorted = partitionHistogram.zip(heavyKeysHistogram).sortBy(-_._2.sum)
          val s2 = sorted.map(_._2)
          println(s"Sorted partition histogram: \n   [${pretty(sorted.map(_._1))}]")
          val m: Int = heavyKeysHistogram.map(_.size).max
          for (i <- 1 to m) {
            println(s"$i: [${pretty(s2.map(l => if (l.size >= i) l(i - 1) else 0.0d))}]")
          }
        }

        // load imbalance (assuming linear processing time for key groups)
        val maxPartitionSize = partitionHistogram.max
        println(s"Iteration $i measurement: $maxPartitionSize")
        balanceMeasurements(i) = maxPartitionSize
      }

      // throw away outliers and average measurements
      val drop = (iterations * cutdown).toInt
      val avg = Mean(balanceMeasurements.sorted.drop(drop).dropRight(drop))
      val metric = avg
      print(s"\nMeasured load imbalance: $metric ")
    }

    // call measureBalance with power-law (zeta) distribution
    measureBalance(Distribution.zeta(exponent, shift, numKeys).probabilities, numPartitions)
  }

  def pretty[A](trav: Traversable[A]): String = {
    trav.map(x=>x.toString.padTo(25, ' ')).mkString(", ")
  }
  // generate a random string key with the seed attached to the end
  def transformKey(k: Int, seed: Int): String = StringGenerator.generateRandomString(k) + seed.toString
}