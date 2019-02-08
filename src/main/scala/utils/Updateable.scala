package utils

trait Updateable[P <: Updateable[P]] extends Partitioner {

  def update(partitioningInfo: PartitioningInfo): P

}