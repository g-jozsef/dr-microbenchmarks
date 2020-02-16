package measurements

import partitioner.Partitioner.PartitionerType
import utils.OptionHandler

trait PartitionerBenchmarkOptionHandler extends OptionHandler {

  private val usage: String =
    """
      | Parameters:
      |   -type: Type of the partitioner, valid types are KeyIsolator, Scan, Redist, Readj, Mixed, Hash
      |   -npart: Number of partitions to use
      |   -nKeys: Number of keys to hash for the partitioner
      |   -iter: Number of times to redo the benchmark
      |   -cutdown: How much of the measurements to cut down (percentage, [0, 1])
      |   -exponent: Exponent of zeta distribution
      |   -shif: Shift of zeta distribution
      |   -kexc: Key excess, Number of heavy keys = keyExcess * numPartitions
      |   -thetaMax: ThetaMax parameter used in MixedPartitioner
      |""".stripMargin

  private val options: OptionFactoryMap = Map(
    "-type" -> ('partitionerType, {
      case "KeyIsolator" => PartitionerType.KeyIsolator
      case "Scan" => PartitionerType.Scan
      case "Redist" => PartitionerType.Redist
      case "Readj" => PartitionerType.Readj
      case "Mixed" => PartitionerType.Mixed
      case "Hash" => PartitionerType.Hash
      case unknown => throw new IllegalArgumentException(s"Unknown partitioner: $unknown")
    }),
    "-npart" -> ('numPartitions, (x: String) => x.toInt),
    "-iter" -> ('iterations, (x: String) => x.toInt),
    "-cutdown" -> ('cutDown, (x: String) => x.toDouble),
    "-tmax" -> ('thetaMax, (x: String) => x.toDouble),
    "-kexc" -> ('keyExcess, (x: String) => x.toInt),
  )

  private val defaultOptions: OptionMap = Map(
    'partitionerType -> PartitionerType.KeyIsolator,
    'numPartitions -> 50,
    'cutDown -> 0.1d,
    'thetaMax -> 0.1d,
    'keyExcess -> 2,
  )

  override def getUsage: String = usage

  override def getOptions: Map[String, (Symbol, String => Any)] = options

  override def getDefaultOptions: OptionMap = defaultOptions
}