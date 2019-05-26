package DataDog

import scala.collection.mutable.Map

trait TParam {

  val CONFIG = "-config"
  val START_DATE = "-StartDate"
  val END_DATE = "-EndDate"
  val HELP = "-h"

  val usage: String = "Usage: cmd " +
    START_DATE + " Start Date (format: 2019-05-01:00)\n" +
    END_DATE + " End Date (format: 2019-05-02:00)\n" +
    CONFIG + " Path of Configuration File\n"
}

object ParseCmdLine extends TParam {

  type OptionMap = Map[String, String]

  def parse(args: Array[String]): Option[OptionMap] = {
    if(args.isEmpty || args(0) == HELP) {
      println(usage); None
    } else {
      val param = Map.empty[String, String]
      try {
        args.indices.foreach{i =>
          args(i) match {
            case START_DATE => param += (START_DATE -> args(i+1))
            case END_DATE => param += (END_DATE -> args(i+1))
            case CONFIG => param += (CONFIG -> args(i+1))
            case _ =>
          }
        }
        Some(param)
      } catch {
        case e: Exception => println(usage); None
      }

    }
  }
}
