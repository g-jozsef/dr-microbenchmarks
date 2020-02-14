package partitioner

/**
  * An updateable partitioner.
  * Such a partitioner's internal state can be updated after each batch of new data, in order to
  * adapt partitioning to changes in key distribution.
  */
trait Updateable[T] extends Partitioner[T] {

  def update(partitioningInfo: PartitioningInfo[T]): Updateable[T]

}
