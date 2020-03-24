package hu.sztaki.microbenchmark.partitioner

trait Adaptive[P <: Adaptive[P, T], T] extends Partitioner[T] {
  def adapt(partitioningInfo: PartitioningInfo[T], newWeighting: Array[Double]): P
}
