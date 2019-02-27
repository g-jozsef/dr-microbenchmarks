package utils

trait Updateable extends Partitioner {

  def update(partitioningInfo: PartitioningInfo): Updateable

}