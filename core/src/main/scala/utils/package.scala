package object utils {
  def computeIf(b: Boolean, f: () => Unit): Boolean = {
    if (b)
      f()
    b
  }

  def pretty[A](padTo: Int, trav: Traversable[A]): String = {
    trav.map(x => x.toString.padTo(padTo, ' ')).mkString(", ")
  }
}
