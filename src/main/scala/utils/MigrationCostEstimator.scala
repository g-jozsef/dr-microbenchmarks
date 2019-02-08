package utils

trait MigrationCostEstimator {

  def getMigrationCostEstimation: Option[Double]

}