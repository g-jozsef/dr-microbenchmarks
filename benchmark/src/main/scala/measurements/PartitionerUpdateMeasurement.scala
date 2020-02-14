//package measurements
//
//import java.nio.ByteBuffer
//
//import com.google.common.hash.Hashing
//import partitioner.GedikPartitioner.GedikPartitioner
//import partitioner.{ConsistentHashPartitioner, KeyIsolatorPartitioner, MixedPartitioner, PartitioningInfo, Updateable}
//import utils._
//
//import scala.util.Random
//
///**
//  * Microbenchmark for measuring update time of different updateable partitioners
//  */
//// Figure 6
//object PartitionerUpdateMeasurement {
//
//  // example arguments: 1000000 1 10 100000 20 KeyIsolator 20 0.1 100 0.1 5
//  def main(args: Array[String]): Unit = {
//    // total number of keys in the hypothetic data
//    val numKeys = args(0).toInt
//    // exponent of key-distribution
//    val exponent = args(1).toDouble
//    // shift parameter of key-distribution
//    val shift = args(2).toDouble
//    // size of a data batch that used for one update of the partitioner
//    // represents microbatches is Spark streaming
//    val batchSize = args(3).toInt
//    // number of batches (= number of consecutive updates)
//    val numBatches = args(4).toInt
//    // name of the partitioner to measure
//    val partitionerName = args(5)
//    // number of partitions of the partitioner
//    val numPartitions = args(6).toInt
//    // partitioner parameter, currently out of use
//    val treeDepthHint = 100
//    // Mixed partitioner parameter, used in the load constraint
//    val mixedThetaMax = args(7).toDouble
//    // KeyIsolatorPartitioner (KIP) parameter, tells how many keys should be regarded as `heavy`
//    // number of heavy keys = keyExcess * numPartitions
//    val keyExcess = 2
//    // on how many iterations should the whole measurement be averaged
//    val iterations = args(8).toInt
//    // measurement cutdown parameter, tells what percentage of measurements should be treated as
//    // outliers
//    val cutdown = args(9).toDouble
//    // ordered keys will be shuffled in groups of this size
//    // this represents gradual random concept drift in the key distribution
//    val keyShuffling = args(10).toInt
//    // total number of keys to be shuffled before an update
//    val shuffleBound = 500
//    // power-law (zeta) key-distribution
//    val distribution = Distribution.zeta(exponent, shift, numKeys)
//
//    // a recordGenerator to generate data-points with random string payload
//    val recordGenerator = new StringKeyedRecordGenerator
//
//    /**
//      * Create the appropriate partitioner based on the partitioner's name.
//      * Valid names are Scan, Redist, Readj, KeyIsolator and Mixed.
//      * All of these partitioners are Updateable.
//      *
//      * @return
//      */
//    def createPartitioner: Updateable = {
//      partitionerName match {
//        case "Scan" | "Redist" | "Readj" =>
//          // assume linear processing time for key-groups
//          val betaS: Double => Double = x => x
//          val betaC: Double => Double = x => x
//          val thetaS = 0.2
//          val thetaC = 0.2
//          val thetaN = 0.2
//          val utility: (Double, Double) => Double = {
//            // optimize for both balance and migration
//            case (balancePenalty, migrationPenalty) => balancePenalty + migrationPenalty
//          }
//
//          // murmurhash library is used for hashing
//          val hash = Hashing.murmur3_32()
//          val hashFunc: Any => Int = {
//            case (x: String) =>
//              hash.hashUnencodedChars(x).asInt()
//            case (x: Int) =>
//              hash.hashInt(x).asInt()
//            case (i: Int, j: Int) =>
//              val bytes = ByteBuffer.allocate(8)
//              bytes.putInt(0, i)
//              bytes.putInt(4, j)
//              hash.hashBytes(bytes.array()).asInt()
//            case _ =>
//              throw new UnsupportedOperationException("Currently we can only hash Int or String.")
//          }
//
//          new GedikPartitioner(numPartitions, 100, hashFunc,
//            betaS, betaC, thetaS, thetaC, thetaN, utility, partitionerName)
//        case "KeyIsolator" =>
//          // internal partitioner (used for partition small keys) is ConsistentHashPartitioner
//          new KeyIsolatorPartitioner[ConsistentHashPartitioner](numPartitions,
//            (pi, weighting) => new ConsistentHashPartitioner(weighting, 100))
//        case "Mixed" =>
//          // max size of hash table set to be the same as for KIP
//          val aMax = keyExcess * numPartitions + 1
//          val partitioner = new MixedPartitioner(numPartitions, aMax, mixedThetaMax)
//          partitioner
//        case _ =>
//          throw new RuntimeException(s"Unknown partitioner: `$partitionerName`")
//      }
//    }
//
//    /**
//      * Calculate the partition histogram by actually partitioning keys with a partitioner
//      * This is not measured; only partitioner update is measured
//      *
//      * @param part
//      * @param keyHistogram
//      * @return
//      */
//    def getPartitionHistogram(part: Updateable, keyHistogram: Seq[(Any, Double)]): Map[Int, Double] = {
//      var partitionHistogram = (0 until numPartitions).map(p => p -> 0.0d).toMap
//      keyHistogram.foreach({ case (k, v) =>
//        val partition = part.getPartition(k)
//        partitionHistogram = partitionHistogram + (partition -> (partitionHistogram(partition) + v))
//      })
//      partitionHistogram
//    }
//
//    // code block #1 to measure; updates partitioner without key-shuffling, but each time a new
//    // sample will be drawn from the key-distribution
//    val block1: CodeBlock = new CodeBlock {
//      // informations needed to construct the partitioner
//      var partitioningInfo: PartitioningInfo = _
//      var partitioner: Updateable = _
//      // total list of keys
//      var keys: Array[Any] = _
//      // key histogram as a list of (key, frequency) pairs
//      var keyHistogram: Seq[(Any, Double)] = _
//      var partitionHistogram: Map[Int, Double] = _
//      // measurement counter, goes from 0 to iterations * (numBatches - 1)
//      var i: Int = 0
//
//      // initialize the next measurement, this is not measured
//      override def init(): Unit = {
//        partitionerName match {
//          case "Mixed" =>
//            // if a new iteration of the measurement has started, reinitialize the partitioner
//            if (i % numBatches == 0) {
//              partitioner = createPartitioner
//              // generate a new set of keys with transformKey
//              keys = (1 to numKeys).map(k => recordGenerator.transformKey(k, Random.nextInt)).toArray[Any]
//            }
//            // generate a new (sorted) key histogram for the current batch
//            keyHistogram = keys.zip(distribution.unorderedEmpiric(batchSize)).filter(_._2 > 0.0d).sortBy(-_._2)
//            // calculate the partitioning histogram for the current batch
//            partitionHistogram = getPartitionHistogram(partitioner, keyHistogram)
//            // calculate the corresponding partitioning info
//            partitioningInfo = PartitioningInfo.newInstance(keyHistogram, numPartitions, treeDepthHint, partitionHistogram = Some(partitionHistogram))
//          // in case of partitioners other than Mixed, we don't need to calculate the
//          // partitioning histogram
//          case _ =>
//            // if a new iteration of the measurement has started, reinitialize the partitioner
//            if (i % numBatches == 0) {
//              partitioner = createPartitioner
//              // generate a new set of keys with transformKey
//              keys = (1 to numKeys).map(k => recordGenerator.transformKey(k, Random.nextInt)).toArray[Any]
//            }
//            // generate a new (sorted) key histogram for the current batch
//            keyHistogram = keys.zip(distribution.unorderedEmpiric(batchSize)).filter(_._2 > 0.0d).sortBy(-_._2)
//            // calculate the corresponding partitioning info
//            partitioningInfo = PartitioningInfo.newInstance(keyHistogram.take(keyExcess * numPartitions + 1), numPartitions, treeDepthHint)
//        }
//        i = i + 1
//      }
//
//      // code to be measured; called immediately after init()
//      override def compute(): Any = {
//        // update the partitioner
//        partitioner = partitioner.update(partitioningInfo)
//        partitioner
//      }
//    }
//
//    // code block #2 to measure; updates partitioner with key-shuffling
//    val block2: CodeBlock = new CodeBlock {
//      // informations needed to construct the partitioner
//      var partitioningInfo: PartitioningInfo = _
//      var partitioner: Updateable = _
//      // total list of keys
//      var keys: Array[Any] = _
//      // key histogram as a list of (key, frequency) pairs
//      var keyHistogram: Seq[(Any, Double)] = _
//      var partitionHistogram: Map[Int, Double] = _
//      // measurement counter, goes from 0 to iterations * (numBatches - 1)
//      var i: Int = 0
//
//      // initialize the next measurement, this is not measured
//      override def init(): Unit = {
//        partitionerName match {
//          case "Mixed" =>
//            // if a new iteration of the measurement has started, reinitialize the partitioner
//            if (i % numBatches == 0) {
//              partitioner = createPartitioner
//              // generate a new set of keys with transformKey
//              keys = (1 to numKeys).map(k => recordGenerator.transformKey(k, Random.nextInt)).toArray[Any]
//            } else {
//              // shuffle the keys; keys are shuffled in a window of size shuffleBound, in
//              // groups of size keyShuffling; the shuffle window is slightly shifted with every
//              // iteration
//              keys = keys.take(i % keyShuffling) ++
//                keys.slice(i % keyShuffling, shuffleBound + i % keyShuffling).grouped(keyShuffling).flatMap(group => Random.shuffle[Any, Seq](group)) ++
//                keys.drop(shuffleBound + i % keyShuffling)
//            }
//            // generate a new (sorted) key histogram for the current batch
//            keyHistogram = keys.zip(distribution.unorderedEmpiric(batchSize)).filter(_._2 > 0.0d).sortBy(-_._2)
//            // calculate the partitioning histogram for the current batch
//            partitionHistogram = getPartitionHistogram(partitioner, keyHistogram)
//            // calculate the corresponding partitioning info
//            partitioningInfo = PartitioningInfo.newInstance(keyHistogram, numPartitions, treeDepthHint, partitionHistogram = Some(partitionHistogram))
//          case _ =>
//            // if a new iteration of the measurement has started, reinitialize the partitioner
//            if (i % numBatches == 0) {
//              partitioner = createPartitioner
//              // generate a new set of keys with transformKey
//              keys = (1 to numKeys).map(k => recordGenerator.transformKey(k, Random.nextInt)).toArray[Any]
//            } else {
//              // shuffle the keys; keys are shuffled in a window of size shuffleBound, in
//              // groups of size keyShuffling; the shuffle window is slightly shifted with every
//              // iteration
//              keys = keys.take(i % keyShuffling) ++
//                keys.slice(i % keyShuffling, shuffleBound + i % keyShuffling).grouped(keyShuffling).flatMap(group => Random.shuffle[Any, Seq](group)) ++
//                keys.drop(shuffleBound + i % keyShuffling)
//            }
//            // generate a new (sorted) key histogram for the current batch
//            keyHistogram = keys.zip(distribution.unorderedEmpiric(batchSize)).filter(_._2 > 0.0d).sortBy(-_._2)
//            // calculate the corresponding partitioning info
//            partitioningInfo = PartitioningInfo.newInstance(keyHistogram.take(keyExcess * numPartitions + 1), numPartitions, treeDepthHint)
//        }
//        i = i + 1
//      }
//
//      // code to be measured; called immediately after init()
//      override def compute(): Any = {
//        // update the partitioner
//        partitioner = partitioner.update(partitioningInfo)
//        partitioner
//      }
//    }
//
//    // measure code block with MicroBenchmarkUtils, print the result
//    println(s"$partitionerName update measurement with key shuffling took ${MicroBenchmarkUtil.measure(block2, iterations * numBatches, Mean, cutdown)} ms " +
//      s"(averaged on $iterations iterations with $cutdown cutdown)")
//  }
//
//  /**
//    * Trait for generating payload for a key
//    *
//    * @tparam R
//    */
//  trait ValueGenerator[R <: Product] {
//    def generateValue(key: Int): R
//  }
//
//  // payload type
//  type ValueTuple = (Double, String)
//
//  /**
//    * Generates constant payload of ValueType
//    */
//  class ConstantValueGenerator extends ValueGenerator[ValueTuple] {
//    override def generateValue(key: Int): (Double, String) =
//      (2424.3254543d, "This is a long-long string without any particular meaning")
//  }
//
//  /**
//    * Abstract class for generating (key, payload) pairs of type (K, R)
//    *
//    * @param valueGenerator
//    * @tparam K
//    * @tparam R
//    */
//  abstract class KeyedRecordGenerator[K, R <: Product](valueGenerator: ValueGenerator[R]) {
//
//    def generateRecord(key: Int): (K, R) = {
//      (transformKey(key), valueGenerator.generateValue(key))
//    }
//
//    // transforms key to type K
//    def transformKey(key: Int): K
//  }
//
//  /**
//    * Generates string-keyed records with constant payload of ValueType
//    */
//  class StringKeyedRecordGenerator extends KeyedRecordGenerator[String, ValueTuple](new ConstantValueGenerator) {
//
//    // generate random string as key
//    override def transformKey(key: Int): String = StringGenerator.generateString(key)
//
//    // generate random string as key, but with control over the seed
//    def transformKey(k: Int, seed: Int): String = StringGenerator.generateString2(k, seed)
//  }
//
//}