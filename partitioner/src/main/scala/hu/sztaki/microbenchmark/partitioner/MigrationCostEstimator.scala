package hu.sztaki.microbenchmark.partitioner

/**
  * Capability of a partitioner to estimate its migration cost
  */
trait MigrationCostEstimator {

  def getMigrationCostEstimation: Option[Double]

}
