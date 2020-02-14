package partitioner

class HashPartitioner[T](override val numPartitions: Int) extends Partitioner[T] {
  require(numPartitions > 0, s"Number of partitions ($numPartitions) should be positive.")

  def getPartition(key: T): Int =
    (key.hashCode % numPartitions + numPartitions) % numPartitions
}