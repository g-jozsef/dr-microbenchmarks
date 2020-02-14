package partitioner

trait Partitioner[T] {
  def numPartitions: Int
  def getPartition(key: T): Int
}
