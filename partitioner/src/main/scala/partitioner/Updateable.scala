package partitioner

import java.nio.ByteBuffer

import com.google.common.hash.Hashing
import partitioner.GedikPartitioner.GedikPartitioner
import partitioner.Partitioner.PartitionerType
import partitioner.Partitioner.PartitionerType.PartitionerType

/**
  * An updateable partitioner.
  * Such a partitioner's internal state can be updated after each batch of new data, in order to
  * adapt partitioning to changes in key distribution.
  */
trait Updateable[T] extends Partitioner[T] {

  def update(partitioningInfo: PartitioningInfo[T]): Updateable[T]

}


object Updateable {

  def apply[T](pType: PartitionerType, numPartitions: Int, keyExcess: Int, thetaMax: Double): Updateable[T] = {
    pType match {
      case PartitionerType.Scan | PartitionerType.Redist | PartitionerType.Readj =>
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
          betaS, betaC, thetaS, thetaC, thetaN, utility, pType.toString)
      case PartitionerType.KeyIsolator =>
        // internal partitioner (used for partition small keys) is ConsistentHashPartitioner
        new KeyIsolatorPartitioner[ConsistentHashPartitioner[T], T](numPartitions,
          (pi, weighting) => new ConsistentHashPartitioner(weighting, 100))
      case PartitionerType.Mixed =>
        // max size of hash table set to be the same as for KIP
        val aMax = keyExcess * numPartitions + 1
        val partitioner = new MixedPartitioner[T](numPartitions, aMax, thetaMax)
        partitioner
      case unknown =>
        throw new RuntimeException(s"Unknown partitioner: `$unknown`")
    }
  }
}