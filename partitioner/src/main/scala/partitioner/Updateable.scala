package partitioner

/**
  * An updateable partitioner.
  * Such a partitioner's internal state can be updated after each batch of new data, in order to
  * adapt partitioning to changes in key distribution.
  */
trait Updateable extends Partitioner {

  def update(partitioningInfo: PartitioningInfo): Updateable

}
