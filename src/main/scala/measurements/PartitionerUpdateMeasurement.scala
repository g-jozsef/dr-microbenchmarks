package measurements

import java.nio.ByteBuffer

import com.google.common.hash.Hashing
import utils.GedikPartitioner.GedikPartitioner
import utils._

import scala.util.Random

// Figure 6
object PartitionerUpdateMeasurement {

  def main(args: Array[String]): Unit = {
    val numKeys = args(0).toInt
    val exponent = args(1).toDouble
    val shift = args(2).toDouble
    val batchSize = args(3).toInt
    val numBatches = args(4).toInt
    val partitionerName = args(5)
    val numPartitions = args(6).toInt
    val treeDepthHint = 100
    val mixedThetaMax = args(7).toDouble
    val keyExcess = 2
    val iterations = args(8).toInt
    val cutdown = args(9).toDouble
    val keyShuffling = args(10).toInt
    val shuffleBound = 500
    val distribution = Distribution.zeta(exponent, shift, numKeys)

    val recordGenerator = new StringKeyedRecordGenerator

    def createPartitioner: Updateable[_] = {
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
          val aMax = keyExcess * numPartitions + 1
          val partitioner = new MixedPartitioner(numPartitions, aMax, mixedThetaMax)
          partitioner
        case _ =>
          throw new RuntimeException(s"Unknown partitioner: `$partitionerName`")
      }
    }

    def getPartitionHistogram(part: Updateable[_], keyHistogram: Seq[(Any, Double)]): Map[Int, Double] = {
      var partitionHistogram = (0 until numPartitions).map(p => p -> 0.0d).toMap
      keyHistogram.foreach({ case (k, v) =>
        val partition = part.getPartition(k)
        partitionHistogram = partitionHistogram + (partition -> (partitionHistogram(partition) + v))
      })
      partitionHistogram
    }

    // update without key shuffling
    val block1: CodeBlock = new CodeBlock {
      var partitioningInfo: PartitioningInfo = _
      var partitioner: Updateable[_] = _
      var keys: Array[Any] = _
      var keyHistogram: Seq[(Any, Double)] = _
      var partitionHistogram: Map[Int, Double] = _
      var i: Int = 0

      // something to do before every compute, but should not be measured
      override def init(): Unit = {
        partitionerName match {
          case "Mixed" =>
            if (i % numBatches == 0) {
              partitioner = createPartitioner
              keys = (1 to numKeys).map(k => recordGenerator.transformKey(k, Random.nextInt)).toArray[Any]
            }
            keyHistogram = keys.zip(distribution.unorderedEmpiric(batchSize)).filter(_._2 > 0.0d).sortBy(-_._2)
            partitionHistogram = getPartitionHistogram(partitioner, keyHistogram)
            partitioningInfo = PartitioningInfo.newInstance(keyHistogram, numPartitions, treeDepthHint, partitionHistogram = Some(partitionHistogram))
          case _ =>
            if (i % numBatches == 0) {
              partitioner = createPartitioner
              keys = (1 to numKeys).map(k => recordGenerator.transformKey(k, Random.nextInt)).toArray[Any]
            }
            keyHistogram = keys.zip(distribution.unorderedEmpiric(batchSize)).filter(_._2 > 0.0d).sortBy(-_._2)
            partitioningInfo = PartitioningInfo.newInstance(keyHistogram.take(keyExcess * numPartitions + 1), numPartitions, treeDepthHint)
        }
        i = i + 1
      }

      // code to be measured
      override def compute(): Any = {
        partitioner = partitioner.update(partitioningInfo).asInstanceOf[Updateable[_]]
        partitioner
      }
    }

    // update with key shuffling
    val block2: CodeBlock = new CodeBlock {
      var partitioningInfo: PartitioningInfo = _
      var partitioner: Updateable[_] = _
      var keys: Array[Any] = _
      var keyHistogram: Seq[(Any, Double)] = _
      var partitionHistogram: Map[Int, Double] = _
      var i: Int = 0

      override def init(): Unit = {
        partitionerName match {
          case "Mixed" =>
            if (i % numBatches == 0) {
              partitioner = createPartitioner
              keys = (1 to numKeys).map(k => recordGenerator.transformKey(k, Random.nextInt)).toArray[Any]
            } else {
              keys = keys.take(i % keyShuffling) ++
                keys.slice(i % keyShuffling, shuffleBound + i % keyShuffling).grouped(keyShuffling).flatMap(group => Random.shuffle[Any, Seq](group)) ++
                keys.drop(shuffleBound + i % keyShuffling)
            }
            keyHistogram = keys.zip(distribution.unorderedEmpiric(batchSize)).filter(_._2 > 0.0d).sortBy(-_._2)
            partitionHistogram = getPartitionHistogram(partitioner, keyHistogram)
            partitioningInfo = PartitioningInfo.newInstance(keyHistogram, numPartitions, treeDepthHint, partitionHistogram = Some(partitionHistogram))
          case _ =>
            if (i % numBatches == 0) {
              partitioner = createPartitioner
              keys = (1 to numKeys).map(k => recordGenerator.transformKey(k, Random.nextInt)).toArray[Any]
            } else {
              keys = keys.take(i % keyShuffling) ++
                keys.slice(i % keyShuffling, shuffleBound + i % keyShuffling).grouped(keyShuffling).flatMap(group => Random.shuffle[Any, Seq](group)) ++
                keys.drop(shuffleBound + i % keyShuffling)
            }
            keyHistogram = keys.zip(distribution.unorderedEmpiric(batchSize)).filter(_._2 > 0.0d).sortBy(-_._2)
            partitioningInfo = PartitioningInfo.newInstance(keyHistogram.take(keyExcess * numPartitions + 1), numPartitions, treeDepthHint)
        }
        i = i + 1
      }

      override def compute(): Any = {
        partitioner = partitioner.update(partitioningInfo).asInstanceOf[Updateable[_]]
        partitioner
      }
    }

    println(s"$partitionerName update measurement with key shuffling took ${MicroBenchmarkUtil.measure(block2, iterations * numBatches, Mean, cutdown)} ms " +
      s"(averaged on $iterations iterations with $cutdown cutdown)")
  }

  trait ValueGenerator[R <: Product] {
    def generateValue(key: Int): R
  }

  type ValueTuple = (Double, String)

  class ConstantValueGenerator extends ValueGenerator[ValueTuple] {
    override def generateValue(key: Int): (Double, String) =
      (2424.3254543d, "This is a long-long string without any particular meaning")
  }

  abstract class KeyedRecordGenerator[K, R <: Product](valueGenerator: ValueGenerator[R]) {

    def generateRecord(key: Int): (K, R) = {
      (transformKey(key), valueGenerator.generateValue(key))
    }

    def transformKey(key: Int): K
  }

  class StringKeyedRecordGenerator extends KeyedRecordGenerator[String, ValueTuple](new ConstantValueGenerator) {

    override def transformKey(key: Int): String = StringGenerator.generateString(key)

    def transformKey(k: Int, seed: Int): String = StringGenerator.generateString2(k, seed)
  }
}