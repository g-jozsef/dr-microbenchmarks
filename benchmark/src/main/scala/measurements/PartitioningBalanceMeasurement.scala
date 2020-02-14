package measurements

import java.nio.ByteBuffer

import com.google.common.hash.Hashing
import partitioner.GedikPartitioner.GedikPartitioner
import partitioner._
import utils._

import scala.reflect.ClassTag

/**
  * Microbenchmark for measuring partitioning balance of different partitioners
  */
// Figure 5, Figure 7
object PartitioningBalanceMeasurement extends OptionHandler {

  override protected val usage: String =
    """
      | PartitioningBalanceMeasurement
      | example parameters: -name KeyIsolator -npart 50 -nkeys 1000 -iter 10 -cutdown 0.1 -exo 1.5 -shift 10 -tmax 0.1
      |
      | Parameters:
      |   -name: Name of the partitioner
      |   -npart: Number of partitions to use
      |   -nKeys: Number of keys to hash for the partitioner
      |   -iter: Number of times to redo the benchmark
      |   -cutdown: How much of the measurements to cut down (percentage, [0, 1])
      |   -exponent: Exponent of zeta distribution
      |   -shif: Shift of zeta distribution
      |   -kexc: Key excess, Number of heavy keys = keyExcess * numPartitions
      |
      |   -thetaMax: ThetaMax parameter used in MixedPartitioner
      |
      |
      |""".stripMargin

  override protected val defaultOptions: PartitioningBalanceMeasurement.OptionMap = Map(
    'partitionerName -> "KeyIsolator",
    'numPartitions -> 50,
    'numKeys -> 1000,
    'iterations -> 10,
    'cutdown -> 0.1d,
    'exponent -> 1.5d,
    'shift -> 10d,
    'thetaMax -> 0.1d,
    'keyExcess -> 2
  )

  override protected val options: Map[String, (Symbol, String => Any)] = Map(
    "-name" -> ('partitionerName, x => x.toString),
    "-npart" -> ('numPartitions, x => x.toInt),
    "-nkeys" -> ('numKeys, x => x.toInt),
    "-iter" -> ('iterations, x => x.toInt),
    "-cutdown" -> ('cutdown, x => x.toDouble),
    "-exp" -> ('exponent, x => x.toDouble),
    "-shift" -> ('shift, x => x.toDouble),
    "-tmax" -> ('thetaMax, x => x.toDouble),
    "-kexc" -> ('keyExcess, x => x.toInt)
  )

  def main(args: Array[String]): Unit = {
    readOptions(args)

    // call measureBalance with power-law (zeta) distribution
    val zeta: Distribution = Distribution.zeta(getOption('exponent), getOption('shift), getOption('numKeys))
    measureBalance(zeta.probabilities, getOption('numPartitions))
  }


  /**
    * Create the appropriate partitioner based on the partitioner's name.
    * Valid names are Hash, Scan, Redist, Readj, KeyIsolator and Mixed.
    * Updateable partitioners will be updated with the key-histogram.
    *
    * @param weights
    * @param numPartitions
    * @return
    */
  def createNewPartitioner[T](weights: Array[(T, Double)], numPartitions: Int)(implicit tag: ClassTag[T]): Partitioner[T] = {
    getOption[String]('partitionerName) match {
      case "Hash" =>
        new HashPartitioner(numPartitions)
      case other =>
        // partitioner parameter, currently out of use
        val treeDepthHint = (Math.log10(1000) / Math.log10(2.0d)).toInt + 2
        // informations needed to construct the partitioner
        val partitioningInfo = PartitioningInfo.newInstance[T](weights.take(getOption[Int]('keyExcess) * numPartitions + 1), numPartitions, treeDepthHint)
        other match {
          case gedik if Seq("Scan", "Redist", "Readj").contains(gedik) =>
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
            new GedikPartitioner[T](numPartitions, 100, hashFunc,
              betaS, betaC, thetaS, thetaC, thetaN, utility, gedik).update(partitioningInfo)
          case "KeyIsolator" =>
            // create KeyIsolatorPartitioner (KIP) and update it
            // internal partitioner (used for partition small keys) is ConsistentHashPartitioner
            new KeyIsolatorPartitioner[ConsistentHashPartitioner[T], T](numPartitions,
              (pi, weighting) => new ConsistentHashPartitioner(weighting, 100)).update(partitioningInfo)
          case "Mixed" =>
            // max size of hash table set to be the same as for KIP
            val aMax = getOption[Int]('keyExcess) * numPartitions + 1
            val thetaMax = getOption[Double]('thetaMax)
            // create Mixed partitioner
            val partitioner = new MixedPartitioner[T](numPartitions, aMax, thetaMax)
            // calculate the partition histogram
            // we need to know the partition histogram for the update
            var partitionHistogram = (0 until numPartitions).map(p => p -> 0.0d).toMap
            weights.foreach {
              case (key, weight) =>
                val part = partitioner.getPartition(key)
                partitionHistogram = partitionHistogram + (part -> (partitionHistogram(part) + weight))
            }
            // Mixed partitioner needs a different partitioning info
            val partitioningInfo2 = PartitioningInfo.newInstance(weights, numPartitions, treeDepthHint,
              partitionHistogram = Some(partitionHistogram))
            // update Mixed partitioner
            partitioner.update(partitioningInfo2)
          case _ =>
            throw new RuntimeException(s"Unknown partitioner: `$other`")
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
    // the best balance possible without knowing the key histogram (use this as a baseline)
    val absoluteBalance = 1.0d / numPartitions
    // the best balance possible with this key histogram (use this as a baseline)
    val distributionBalance = Math.max(absoluteBalance, keyHistogram.head)

    val iterations = getOption[Int]('iterations)
    val balanceMeasurements = new Array[Double](iterations)
    for (i <- 0 until iterations) {
      println(s"\nRunning iteration $i...")
      val partitionHistogram = Array.fill[Double](numPartitions)(0.0d)

      // create keys; new random keys must be created for every measurement to average out
      // the effects of hashing
      val weights = Vector.tabulate(keyHistogram.length)(i => (transformKey(i, i), keyHistogram(i))).toArray

      // create the partitioner
      val partitioner = createNewPartitioner[String](weights, numPartitions)

      // calculate the partition histogram
      weights foreach {
        case (key, weight) =>
          partitionHistogram(partitioner.getPartition(key)) += weight
      }

      // this block of code is optional; it prints a sorted list of heavy keys for each
      // partition; good for analyzing heavy-key balance and lightweight-key balance separately
      if (getOption[String]('partitionerName) == "KeyIsolator") {
        val heavyKeys = weights.map(x=>x._1).take(getOption[Int]('keyExcess) * numPartitions)
        println(s"Heavy keys: [${heavyKeys.mkString(", ")}]")
        var heavyKeysHistogram = Array.fill[List[Double]](numPartitions)(List.empty[Double])
        partitioner.asInstanceOf[KeyIsolatorPartitioner[ConsistentHashPartitioner[String], String]]
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
    val drop = (iterations * getOption[Double]('cutdown)).toInt
    val avg = Mean(balanceMeasurements.sorted.drop(drop).dropRight(drop))
    val metric = avg
    print(s"\nMeasured load imbalance: $metric ")
  }


  def pretty[A](trav: Traversable[A]): String = {
    trav.map(x => x.toString.padTo(25, ' ')).mkString(", ")
  }

  // generate a random string key with the seed attached to the end
  def transformKey(k: Int, seed: Int): String = StringGenerator.generateRandomString(k) + seed.toString
}