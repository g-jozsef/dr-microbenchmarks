package utils

import scala.util.Random

object MicroBenchmarkUtil {

  private val warmupFactor = 0.3d
  private val headCut = 0.5d

  def measureQuantiles(block: CodeBlock, iterations: Int, quantiles: Double*): Array[Double] = {
    val measurements = measure(block, iterations)
    val size = measurements.size
    val sorted = measurements.sorted
    quantiles.map(q => sorted((size * q / 100).toInt)).toArray
  }

  def measure(block: CodeBlock, iterations: Int, metric: Metric): Double = {
    metric(measure(block, iterations))
  }

  def measure(block: CodeBlock, iterations: Int, metric: Metric, cutDown: Double): Double = {
    val measurements = measure(block, iterations)
    val size = measurements.size
    val drop = (size * cutDown).toInt
    metric(measurements.sorted.drop(drop).dropRight(drop))
  }

  def measure(block: CodeBlock, iterations: Int, cutDown: Double): Seq[Double] = {
    val measurements = measure(block, iterations)
    val size = measurements.size
    val drop = (size * cutDown).toInt
    measurements.sorted.drop(drop).dropRight(drop)
  }

  def measure(block: CodeBlock, iterations: Int): Seq[Double] = {
    System.gc()

    // warm-up phase
    val warmup = for (_ <- 1 to (iterations * warmupFactor).toInt) yield {
      measure(block)
    }
    var i: Int = Random.nextInt((iterations * warmupFactor).toInt)
    println("Warmup dummy print: " + warmup(i)._2)
    println("Warmup random measurement: " + warmup(i)._1 + "ms")

    System.gc()

    // timing phase
    val timing = for (_ <- 1 to iterations) yield {
      measure(block)
    }
    i = Random.nextInt(iterations)
    println("Timing dummy print: " + timing(i)._2)

    System.gc()

    timing.drop((iterations * headCut).toInt).map(_._1)
  }

  def measure(block: CodeBlock): (Double, Any) = {
    block.init()

    val t0 = System.nanoTime()
    val b = block.compute()
    val t1 = System.nanoTime()
    ((t1 - t0).toDouble / 1000000, b)
  }
}

trait CodeBlock {
  def init(): Unit

  def compute(): Any
}

sealed trait Metric {
  def apply(measured: Seq[Double]): Double
}

object Mean extends Metric {
  override def apply(measured: Seq[Double]): Double = measured.sum / measured.length
}

object Max extends Metric {
  override def apply(measured: Seq[Double]): Double = measured.max
}

object Min extends Metric {
  override def apply(measured: Seq[Double]): Double = measured.min
}