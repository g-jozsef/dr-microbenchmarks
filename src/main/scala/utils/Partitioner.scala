package utils

trait Partitioner extends Serializable {

  val id: String = StringGenerator.generateId()

  def numPartitions: Int

  def getPartition(key: Any): Int

}