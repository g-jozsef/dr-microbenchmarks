package measurements

import partitioner.Partitioner.PartitionerType
import partitioner.Partitioner.PartitionerType.PartitionerType
import partitioner._
import utils.ValueGenerator.{KeyedRecordGenerator, StringKeyedRecordGenerator, ValueTuple}
import utils._

import scala.reflect.ClassTag

/**
  * Microbenchmark for measuring partitioning balance of different partitioners
  */
// Figure 5, Figure 7
object PartitioningBalanceMeasurement extends PartitionerBenchmarkOptionHandler {
  private val usage: String =
    s"""
       | PartitioningBalanceMeasurement
       | example parameters: -name KeyIsolator -npart 50 -nkeys 1000 -iter 10 -cutdown 0.1 -exp 1.5 -shift 10 -tmax 0.1
       |
       | ${super.getUsage}
       |
       |""".stripMargin

  private val defaultOptions: OptionMap = super.getDefaultOptions ++ Map(
    'numKeys -> 1000,
    'iterations -> 10,
    'shift -> 10d,
    'exponent -> 1.5d
  )

  private val options: OptionFactoryMap = super.getOptions ++ Map(
    "-nkeys" -> ('numKeys, (x: String) => x.toInt),
    "-exp" -> ('exponent, (x: String) => x.toDouble),
    "-shift" -> ('shift, (x: String) => x.toDouble),
  )

  override def getUsage: String = usage

  override def getDefaultOptions: OptionMap = defaultOptions

  def main(args: Array[String]): Unit = {
    readOptions(args)

    // call measureBalance with power-law (zeta) distribution
    val zeta: Distribution = Distribution.zeta(getOption('exponent), getOption('shift), getOption('numKeys))
    measureBalance[String](getOption('partitionerType),
      zeta.probabilities,
      new StringKeyedRecordGenerator(),
      getOption('numPartitions),
      getOption('keyExcess),
      getOption('thetaMax),
      getOption('iterations),
      getOption('cutDown))
  }

  /**
    * The measurement code. Creates the partitioner and measures partitioning balance under the
    * assumption that the key-distribution is static in time. Results are averaged through many
    * iterations.
    *
    * @param keyHistogram
    * @param numPartitions
    */
  def measureBalance[T](partitionerType: PartitionerType,
                        keyHistogram: Array[Double],
                        recordGenerator: KeyedRecordGenerator[T, ValueTuple],
                        numPartitions: Int,
                        keyExcess: Int,
                        thetaMax: Double,
                        iterations: Int,
                        cutDown: Double)
                       (implicit tag: ClassTag[T]): Unit = {
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
      val weights = Vector.tabulate(keyHistogram.length)(i => (recordGenerator.transformKey(i, i.toString), keyHistogram(i))).toArray

      // create the partitioner
      var partitioner = Partitioner[T](partitionerType,
        numPartitions,
        keyExcess,
        thetaMax)

      partitioner match {
        case updatable: Updateable[T] =>
          partitioner = Partitioner.update[T](updatable, weights, keyExcess, numPartitions)
        case _ =>
      }

      // calculate the partition histogram
      weights foreach {
        case (key, weight) =>
          partitionHistogram(partitioner.getPartition(key)) += weight
      }

      // this block of code is optional; it prints a sorted list of heavy keys for each
      // partition; good for analyzing heavy-key balance and lightweight-key balance separately
      if (partitionerType == PartitionerType.KeyIsolator) {
        val heavyKeys = weights.map(x => x._1).take(keyExcess * numPartitions)
        println(s"Heavy keys: [${heavyKeys.mkString(", ")}]")
        var heavyKeysHistogram = Array.fill[List[Double]](numPartitions)(List.empty[Double])
        partitioner.asInstanceOf[KeyIsolatorPartitioner[ConsistentHashPartitioner[String], String]]
          .heavyKeysMap.foreach({ case (k, p) => heavyKeysHistogram(p) :+= keyHistogram(heavyKeys.indexOf(k)) })
        heavyKeysHistogram = heavyKeysHistogram.map(l => l.sortBy(-_))
        val sorted = partitionHistogram.zip(heavyKeysHistogram).sortBy(-_._2.sum)
        val s2 = sorted.map(_._2)
        println(s"Sorted partition histogram: \n   [${pretty(25, sorted.map(_._1))}]")
        val m: Int = heavyKeysHistogram.map(_.size).max
        for (i <- 1 to m) {
          println(s"$i: [${pretty(25, s2.map(l => if (l.size >= i) l(i - 1) else 0.0d))}]")
        }
      }

      // load imbalance (assuming linear processing time for key groups)
      val maxPartitionSize = partitionHistogram.max
      println(s"Iteration $i measurement: $maxPartitionSize")
      balanceMeasurements(i) = maxPartitionSize
    }

    // throw away outliers and average measurements
    val drop = (iterations * cutDown).toInt
    val avg = Mean(balanceMeasurements.sorted.drop(drop).dropRight(drop))
    val metric = avg
    print(s"\nMeasured load imbalance: $metric ")
  }
}