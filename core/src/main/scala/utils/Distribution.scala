package utils

import scala.util.Random

/**
  * An ordered discrete probability distribution
  *
  * @param probabilities the probabilities of the distribution
  */
class Distribution(val probabilities: Array[Double]) extends Serializable {

  import Distribution._

  // number of possible outcomes
  val width = probabilities.length
  // precision to counteract numeric error
  val precision = width.toDouble / 1000000

  // sanity checks
  for (i <- 0 until width - 1) {
    assert(probabilities(i) >= probabilities(i + 1) - precision)
  }
  assert(probabilities.isEmpty || probabilities.last >= 0 - precision)

  // cumulative distribution
  val aggregated = probabilities.scan(0.0d)(_ + _).drop(1)
  // sanity check
  assert(Math.abs(aggregated.last - 1) <= precision)

  // get probability at specific index
  def get(index: Int): Double = probabilities(index - 1)

  // get a random outcome of this distribution
  def sample(): Int = {
    val rnd = 1 - Random.nextDouble()
    binarySearch(aggregated, rnd) + 1
  }

  // translate x in [0, 1] to outcome
  def sample(x: Double): Int = {
    assert(x >= 0 && x <= 1)
    binarySearch(aggregated, x) + 1
  }

  // draw a sample from the distribution, get it as a new Distribution
  def empiric(sampleSize: Int): Distribution = {
    val empiric = Array.fill[Double](width)(0.0d)
    for (i <- 1 to sampleSize) {
      empiric(sample() - 1) += 1.0d
    }
    for (i <- 0 until width) {
      empiric(i) /= sampleSize
    }
    new Distribution(empiric.sortBy(-_))
  }

  // draw a sample from the distribution, and get it in bare (unordered) form
  def unorderedEmpiric(sampleSize: Int): Array[Double] = {
    val empiric = Array.fill[Double](width)(0.0d)
    for (i <- 1 to sampleSize) {
      empiric(sample() - 1) += 1.0d
    }
    for (i <- 0 until width) {
      empiric(i) /= sampleSize
    }
    empiric
  }

  // probabilities
  override def toString: String = {
    s"Distribution${probabilities.mkString("(", ", ", ")")}"
  }
}

// Constructors for frequently used distributions
object Distribution {

  // helper algorithm
  def binarySearch(array: Array[Double], value: Double): Int = {
    binarySearch((i: Int) => array(i), value, 0, array.length - 1)
  }

  def binarySearch(f: Int => Double, value: Double, lower: Int, upper: Int): Int = {
    if (lower == upper) {
      lower
    } else {
      val middle = (lower + upper) / 2
      if (value <= f(middle)) {
        binarySearch(f, value, lower, middle)
      } else {
        binarySearch(f, value, middle + 1, upper)
      }
    }
  }

  // exponential distribution with parameter lambda, cut-down at width
  def exponential(lambda: Double, width: Int): Distribution = {
    assert(lambda >= 0.0d && lambda <= 1.0d)
    assert(width >= 1)
    val normalizer = if (lambda < 1) (1.0d - Math.pow(lambda, width)) / (1.0d - lambda) else width
    val probabilities = Array.tabulate[Double](width)(i => Math.pow(lambda, i) / normalizer)
    new Distribution(probabilities)
  }

  // uniform distribution
  def uniform(width: Int): Distribution = {
    assert(width >= 1)
    val probabilities = Array.fill[Double](width)(1.0d / width)
    new Distribution(probabilities)
  }

  // dirac delta distribution
  def dirac(width: Int): Distribution = {
    assert(width >= 1)
    val probabilities = Array.tabulate[Double](width)(i => if (i == 0) 1 else 0)
    new Distribution(probabilities)
  }

  // linear distribution; probabilities decrease linearly
  def linear(spread: Int, width: Int): Distribution = {
    assert(width >= 1 && spread >= 1)
    val normalizer = if (spread >= width) (width * (2 * spread - width + 1)).toDouble / 2 else (spread * (spread + 1)).toDouble / 2
    val probabilities = Array.tabulate[Double](width)(i => if (i < spread) (spread - i) / normalizer else 0.0d)
    new Distribution(probabilities)
  }

  // zeta (power-law) distribution with exponent and shift parameters, cut down at width
  def zeta(exponent: Double, shift: Double, width: Int): Distribution = {
    assert(width >= 1)
    assert(exponent >= 0)
    assert(shift > 0)
    val values = Array.tabulate[Double](width)(i => Math.pow(i + shift, -exponent))
    val normalizer = values.sum
    val probabilities = values.map(_ / normalizer)
    new Distribution(probabilities)
  }

  // two-step distribution; the first `spread` probabilities are the same, the others are 0.0
  def twoStep(spread: Double, width: Int): Distribution = {
    assert(width >= 1)
    assert(spread >= 0 && spread <= width)
    val height: Double = 1.0d / spread
    val remainder = (spread - spread.floor) / spread
    val probabilities = Array.tabulate[Double](width)(i =>
      if (i < spread.floor) {
        height
      } else if (i == spread.floor) {
        remainder
      } else {
        0.0d
      })
    new Distribution(probabilities)
  }
}