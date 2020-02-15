package utils

trait OptionHandler {
  type OptionMap = Map[Symbol, Any]
  type OptionFactoryMap = Map[String, (Symbol, String => Any)]

  private var optionsMap: Option[OptionMap] = None


  def getUsage: String
  def getDefaultOptions: OptionMap
  def getOptions: OptionFactoryMap

  final def readOptions(args: Array[String]): Unit = {
    try {
      optionsMap = Some(nextOption(Map(), args.toList))
      var missing = Seq.empty[String]
      if (!getOptions.values.forall {
        case (symbol: Symbol, _) =>
          val ok = optionsMap.get.contains(symbol) || getDefaultOptions.contains(symbol)
          if(!ok) {
            missing ++= Seq(getOptions.find(x=>x._2._1 == symbol).get._1)
          }
          ok
      })
        throw new IllegalArgumentException(s"Not all required parameters have values!\nMissing parameters are: {${missing.mkString(",")}]")
    } catch {
      case ex: IllegalArgumentException =>
        println(ex.getMessage)
        println(getUsage)
        System.exit(1)
        Map()
    }
  }

  @scala.annotation.tailrec
  final def nextOption(map: OptionMap, params: List[String]): OptionMap = params match {
    case Nil =>
      map
    case "-h" :: _ | "-help" :: _ =>
      println(getUsage)
      System.exit(0)
      Map()
    case key :: value :: tail if getOptions.contains(key) =>
      val symbol = getOptions(key)._1
      val transformedValue = getOptions(key)._2(value)
      nextOption(map ++ Map(symbol -> transformedValue), tail)
    case option :: _ =>
      throw new IllegalArgumentException("Unknown option " + option)
  }



  final def getOption[T](symbol: Symbol): T = {
    if(optionsMap.isEmpty) {
      throw new IllegalStateException("No options found, did you forget to call readOptions?")
    }
    if (optionsMap.get.contains(symbol))
      optionsMap.get(symbol).asInstanceOf[T]
    else if (getDefaultOptions.contains(symbol))
      getDefaultOptions(symbol).asInstanceOf[T]
    else {
      println("Missing required option: " + symbol.toString())
      println(getUsage)
      System.exit(1)
      val option = getOptions.find(x=>x._2._1 == symbol)
      if(option.isEmpty)
        throw new IllegalArgumentException("Unknown symbol: " + symbol.toString())
      else
        throw new IllegalArgumentException("Missing required option: " + option.get._1)
    }
  }
}