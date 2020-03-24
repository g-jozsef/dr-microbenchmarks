package hu.sztaki.microbenchmark.utils

import scala.util.Random

// Utils for micro-benchmarking runtime measurements
object MicroBenchmarkUtil {

  // ratio of the warmup phase
  private val warmupFactor = 0.3d
  // drop this fraction of measurements away (on top of the warmup factor)
  private val headCut = 0.5d

  // measure code block `iterations` times and return the specified quantiles
  def measureQuantiles(block: CodeBlock, iterations: Int, quantiles: Double*): Array[Double] = {
    val measurements = measure(block, iterations)
    val size = measurements.size
    val sorted = measurements.sorted
    quantiles.map(q => sorted((size * q / 100).toInt)).toArray
  }

  // measure a code block `iterations` times and calculate a metric on it
  def measure(block: CodeBlock, iterations: Int, metric: Metric): Double = {
    metric(measure(block, iterations))
  }

  // measure a code block `iterations` times with outlier cutdown, and calculate a metric on it
  def measure(block: CodeBlock, iterations: Int, metric: Metric, cutDown: Double): Double = {
    val measurements = measure(block, iterations)
    val size = measurements.size
    val drop = (size * cutDown).toInt
    metric(measurements.sorted.drop(drop).dropRight(drop))
  }

  // measure a code block `iterations` times with outlier cutdown
  def measure(block: CodeBlock, iterations: Int, cutDown: Double): Seq[Double] = {
    val measurements = measure(block, iterations)
    val size = measurements.size
    val drop = (size * cutDown).toInt
    measurements.sorted.drop(drop).dropRight(drop)
  }

  // measure a code block `iterations` times; the results are in milliseconds
  // the code block can be any block of scala code
  def measure(block: CodeBlock, iterations: Int): Seq[Double] = {
    // collect garbage
    System.gc()

    // warm-up phase
    // we need this because jvm optimizations kick in after some run cycle
    val warmup = for (_ <- 1 to (iterations * warmupFactor).toInt) yield {
      measure(block)
    }
    // dummy print
    var i: Int = Random.nextInt((iterations * warmupFactor).toInt)
    println("Warmup dummy print: " + warmup(i)._2)
    println("Warmup random measurement: " + warmup(i)._1 + "ms")

    System.gc()

    // timing phase (real measurement)
    val timing = for (_ <- 1 to iterations) yield {
      measure(block)
    }
    // dummy print
    i = Random.nextInt(iterations)
    println("Timing dummy print: " + timing(i)._2)

    System.gc()

    // drop some of the earliest measurements of the timing phase
    timing.drop((iterations * headCut).toInt).map(_._1)
  }

  // measure a code block once in milliseconds
  def measure(block: CodeBlock): (Double, Any) = {
    block.init()

    val t0 = System.nanoTime()
    val b = block.compute()
    val t1 = System.nanoTime()
    ((t1 - t0).toDouble / 1000000, b)
  }
}

// measurement code should extend this trait
trait CodeBlock {
  // initialize measurement
  def init(): Unit

  // the code block to measure
  def compute(): Any
}

// trait for metrics
sealed trait Metric {
  def apply(measured: Seq[Double]): Double
}

// mean of measurements
object Mean extends Metric {
  override def apply(measured: Seq[Double]): Double = measured.sum / measured.length
}

// maximum of measurements
object Max extends Metric {
  override def apply(measured: Seq[Double]): Double = measured.max
}

// minimum of measurements
object Min extends Metric {
  override def apply(measured: Seq[Double]): Double = measured.min
}