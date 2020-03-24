package hu.sztaki.microbenchmark.partitioner

class RoundRobinPartitioner[T](override val numPartitions: Int) extends Partitioner[T] {
  require(numPartitions > 0, s"Number of partitions ($numPartitions) should be positive.")

  var processedElements: Int = -1

  def getPartition(key: T): Int = {
    processedElements += 1
    processedElements % numPartitions
  }
}