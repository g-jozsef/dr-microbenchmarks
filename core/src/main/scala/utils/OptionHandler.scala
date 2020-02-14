package utils

trait OptionHandler {
  type OptionMap = Map[Symbol, Any]

  protected val usage: String =
    """
      |
      |""".stripMargin
  protected val defaultOptions: OptionMap
  protected val options: Map[String, (Symbol, String => Any)]
  private var optionsMap: Option[OptionMap] = None

  final def readOptions(args: Array[String]): Unit = {
    try {
      optionsMap = Some(nextOption(Map(), args.toList))
      if (!options.values.forall {
        case (symbol: Symbol, _) =>
          optionsMap.get.contains(symbol) || defaultOptions.contains(symbol)
      })
        throw new IllegalArgumentException("Not all required parameters have values!")
    } catch {
      case ex: IllegalArgumentException =>
        println(ex.getMessage)
        println(usage)
        System.exit(1)
        Map()
    }
  }

  @scala.annotation.tailrec
  final def nextOption(map: OptionMap, params: List[String]): OptionMap = params match {
    case Nil =>
      map
    case key :: value :: tail if options.contains(key) =>
      val symbol = options(key)._1
      val transformedValue = options(key)._2(value)
      nextOption(map ++ Map(symbol -> transformedValue), tail)
    case option :: _ =>
      throw new IllegalArgumentException("Unknown option " + option)
  }



  final def getOption[T](symbol: Symbol): T = {
    if (optionsMap.get.contains(symbol))
      optionsMap.get(symbol).asInstanceOf[T]
    else if (defaultOptions.contains(symbol))
      defaultOptions(symbol).asInstanceOf[T]
    else {
      println("Missing required option: " + symbol.toString())
      println(usage)
      System.exit(1)
      throw new IllegalArgumentException("Missing required option: " + symbol.toString())
    }
  }
}
