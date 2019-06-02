package DataDog

import java.io.File
import scala.collection.mutable.Map

trait TFileUtils {
  /** Build a path string from a list of strings
    *
    * @param segs A list of strings
    * @return The string of the path
    */
  def buildPath(segs: List[String]): String = new File(segs.mkString(File.separator)).getPath

}

trait TParam {

  val CONFIG = "-config"
  val START_DATE = "-StartDate"
  val END_DATE = "-EndDate"
  val DATA = "-Data"
  val RESULT = "-Result"
  val BLACK_LIST = "-BlackList"
  val HELP = "-h"

  val usage: String = "Usage: cmd " +
    START_DATE + " Start date (format: 2019-05-01:00) - mandatory\n" +
    END_DATE + " End date (format: 2019-05-02:00) - mandatory\n" +
    DATA + " Path of downloaded data\n" +
    BLACK_LIST + "Path of blacklist file\n"
    RESULT + " Path to save the result\n" +
    CONFIG + " Path of configuration file (Coming soon)"
}

object SystemConfig extends TParam {

  type OptionMap = Map[String, String]

  protected[DataDog] var Params: OptionMap = Map.empty[String, String]

  /** Parsing the command line
    *
    * @param args The input command line
    * @return A Map with all extracted configurations
    */
  def parseCmdLine(args: Array[String]): Option[OptionMap] = {
    if(args.isEmpty || args(0) == HELP) {
      println(usage); None
    } else {
      try {
        args.indices.foreach{i =>
          args(i) match {
            case START_DATE => Params += (START_DATE -> args(i+1))
            case END_DATE => Params += (END_DATE -> args(i+1))
            case DATA => Params += (DATA -> args(i+1))
            case BLACK_LIST => Params += (BLACK_LIST -> args(i+1))
            case RESULT => Params += (RESULT -> args(i+1))
            case CONFIG => Params += (CONFIG -> args(i+1))
            case _ =>
          }
        }
        if(Params.contains(START_DATE) && Params.contains(END_DATE)) Some(Params)
        else { println(usage); None}
      } catch {
        case e: Exception => println(usage); None
      }
    }
  }
}
