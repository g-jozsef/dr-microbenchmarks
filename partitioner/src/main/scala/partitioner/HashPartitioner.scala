package partitioner

class HashPartitioner[T](override val numPartitions: Int) extends Partitioner[T] {
  def getPartition(key: T): Int = {
      val rawMod = key.hashCode % numPartitions
      rawMod + (if (rawMod < 0) numPartitions else 0)
  }
}