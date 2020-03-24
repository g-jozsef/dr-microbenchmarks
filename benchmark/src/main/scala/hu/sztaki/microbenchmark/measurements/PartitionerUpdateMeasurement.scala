package hu.sztaki.microbenchmark.measurements

import hu.sztaki.microbenchmark.utils.{CodeBlock, Distribution, Mean, MicroBenchmarkUtil}
import hu.sztaki.microbenchmark.partitioner.Partitioner.PartitionerType
import hu.sztaki.microbenchmark.partitioner.Partitioner.PartitionerType.PartitionerType
import hu.sztaki.microbenchmark.partitioner.Updateable
import hu.sztaki.microbenchmark.utils.ValueGenerator.StringKeyedRecordGenerator

import scala.util.Random

/**
  * Microbenchmark for measuring update time of different updateable partitioners
  */
// Figure 6
object PartitionerUpdateMeasurement extends PartitionerBenchmarkOptionHandler {
  private val usage: String =
    s"""
       | PartitionerUpdateMeasurement
       | example parameters: -name KeyIsolator -npart 50 -nkeys 1000000 -iter 100 -cutdown 0.1 -exp 1 -shift 10 -tmax 0.1
       |
       | ${super.getUsage}
       |
       |  -m: 1 = updates partitioner without key-shuffling, but each time a new sample will be drawn from the key-distribution
       |      2 = updates partitioner with key-shuffling
       |
       |  -batchsize: size of a data batch that used for one update of the partitioner, represents microbatches is Spark streaming
       |  -nbatch: number of batches (= number of consecutive updates)
       |
       |
       |   only needed when using shuffled block
       |  -keyshuf: ordered keys will be shuffled in groups of this size this represents gradual random concept drift in the key distribution
       |  -shufflebound: total number of keys to be shuffled before an update
       |""".stripMargin

  private val defaultOptions: OptionMap = super.getDefaultOptions ++ Map(
    'numKeys -> 1000000,
    'iterations -> 100,
    'exponent -> 1d,
    'shift -> 10d,

    'batchSize -> 100000,
    'numBatches -> 20,
    'blockToUse -> 1,

    'keyShuffling -> 10,
    'shuffleBound -> 500
  )

  private val options = super.getOptions ++ Map(
    "-nkeys" -> ('numKeys, (x: String) => x.toInt),
    "-batchsize" -> ('batchSize, (x: String) => x.toInt),
    "-nbatch" -> ('numBatches, (x: String) => x.toInt),
    "-keyshuf" -> ('keyShuffling, (x: String) => x.toInt),
    "-shufflebound" -> ('shuffleBound, (x: String) => x.toInt),
    "-m" -> ('blockToUse, (x: String) => x.toInt)
  )

  override def getUsage: String = usage

  override def getDefaultOptions: PartitionerUpdateMeasurement.OptionMap = defaultOptions

  override def getOptions: Map[String, (Symbol, String => Any)] = options

  def main(args: Array[String]): Unit = {
    readOptions(args)
    val exponent: Double = getOption('exponent)
    val shift: Double = getOption('shift)
    val numKeys: Int = getOption('numKeys)
    val blockToUse: Int = getOption[Int]('blockToUse)
    val partitionerType: PartitionerType = getOption('partitionerType)
    val numBatches: Int = getOption('numBatches)
    val batchSize: Int = getOption('batchSize)
    val numPartitions: Int = getOption('numPartitions)
    val keyExcess: Int = getOption('keyExcess)
    val thetaMax: Double = getOption('thetaMax)
    val iterations = getOption[Int]('iterations)
    val cutdown = getOption[Double]('cutDown)

    // power-law (zeta) key-distribution
    val distribution = Distribution.zeta(exponent, shift, numKeys)

    val stringGenerator = new StringKeyedRecordGenerator()

    val codeBlock: CodeBlock =
      blockToUse match {
        case 1 =>
          // code block #1 to measure; updates partitioner without key-shuffling, but each time a new
          // sample will be drawn from the key-distribution
          new PartitionerCodeBlock[String](
            distribution,
            stringGenerator,
            partitionerType,
            numBatches,
            batchSize,
            numPartitions,
            numKeys,
            keyExcess,
            thetaMax
          ) {
            // initialize the next measurement, this is not measured
            override def init(): Unit = {
              initPartitiner()
              initPartitioningInfo()
              i = i + 1
            }
          }

        case 2 =>
          val keyShuffling: Int = getOption('keyShuffling)
          val shuffleBound: Int = getOption('shuffleBound)
          // code block #2 to measure; updates partitioner with key-shuffling
          new PartitionerCodeBlock[String](
            distribution,
            stringGenerator,
            partitionerType,
            numBatches,
            batchSize,
            numPartitions,
            numKeys,
            keyExcess,
            thetaMax
          ) {
            // initialize the next measurement, this is not measured
            override def init(): Unit = {
              initPartitiner()
              // if a new iteration of the measurement has started, reinitialize the partitioner
              if (i % numBatches != 0) {
                // shuffle the keys; keys are shuffled in a window of size shuffleBound, in
                // groups of size keyShuffling; the shuffle window is slightly shifted with every
                // iteration
                keys = keys.take(i % keyShuffling) ++
                  keys.slice(i % keyShuffling, shuffleBound + i % keyShuffling).grouped(keyShuffling).flatMap(group => Random.shuffle[String, Seq](group)) ++
                  keys.drop(shuffleBound + i % keyShuffling)
              }
              initPartitioningInfo()
              i = i + 1
            }
          }
      }

    // measure code block with MicroBenchmarkUtils, print the result
    val measurement = MicroBenchmarkUtil.measure(codeBlock, iterations * numBatches, Mean, cutdown)
    println(s"${getOption[String]('partitionerType)} update measurement with key shuffling took $measurement ms " +
      s"(averaged on $iterations iterations with $cutdown cutdown)")
  }

  /**
    * Calculate the partition histogram by actually partitioning keys with a partitioner
    * This is not measured; only partitioner update is measured
    *
    * @param part
    * @param keyHistogram
    * @return
    */
  def getPartitionHistogram[T](part: Updateable[T], keyHistogram: Seq[(T, Double)]): Map[Int, Double] = {
    var partitionHistogram = (0 until getOption('numPartitions)).map(p => p -> 0.0d).toMap
    keyHistogram.foreach({ case (k, v) =>
      val partition = part.getPartition(k)
      partitionHistogram = partitionHistogram + (partition -> (partitionHistogram(partition) + v))
    })
    partitionHistogram
  }

}