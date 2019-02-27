package utils

object AdvancedBinarySearch {

  // finds position of value in vector
  // if value is not in vector, it finds the insertion position
  def binarySearch[T](vector: Vector[T], value: T, comparator: T => Double): Int = {
    if (comparator(value) > comparator(vector.last)) {
      vector.length
    } else {
      binarySearch[T](vector, value, 0, vector.length - 1, comparator)
    }
  }

  private def binarySearch[T](vector: Vector[T], value: T, lower: Int, upper: Int, comparator: T => Double): Int = {
    if (lower == upper) {
      lower
    } else {
      val middle = (lower + upper) / 2
      if (comparator(value) <= comparator(vector(middle))) {
        binarySearch(vector, value, lower, middle, comparator)
      } else {
        binarySearch(vector, value, middle + 1, upper, comparator)
      }
    }
  }
}