package object utils {
  def computeIf(b: Boolean, f: () => Unit): Boolean = {
    if (b)
      f()
    b
  }
}
