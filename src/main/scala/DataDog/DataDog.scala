package DataDog

import sys.process._
import java.net.URL

import scala.io.Source
import java.net.URL
import java.io.{File, FileOutputStream, FileWriter}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.hadoop.fs
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Hours}
import org.apache.hadoop.fs.Path

import DatadogEncoders._
import SystemConfig._


object DataDog extends TParam with TFileUtils {

  lazy val conf = new SparkConf()
  lazy val spark = SparkSession.builder.config(conf).getOrCreate()
  lazy val sc = spark.sparkContext
  /** Path of the BLACK LIST File*/
  val BLACK_LIST = "BLACK_LIST_PATH"
  /** Link of pageviews */
  final val PAGEVIEWS = "https://dumps.wikimedia.org/other/pageviews/"
  /** Path of download files */
  lazy val DATA_PATH = Params.getOrElse(DATA, System.getProperty("user.dir"))

  def fileDownloader(url: String): String = {
    try {
      val filename = url.split("/").last
      println(filename)
      val path = buildPath(DATA_PATH +: filename +: Nil)
      println(path)
      new URL(url) #> new File(path) !!;
      path
    } catch {
      case e: Throwable => println("Fail to Download the File")
        e.printStackTrace()
        ""
    }
  }

  /** Entry of the application */
  def main(args: Array[String]): Unit = {

    SystemConfig.parseCmdLine(args).map{ _ =>
      val startDate = DateTime.parse(Params.getOrElse(START_DATE, ""), DateTimeFormat.forPattern("yyyy-MM-dd:HH"))
      val endDate = DateTime.parse(Params.getOrElse(END_DATE, ""), DateTimeFormat.forPattern("yyyy-MM-dd:HH"))
      val hoursCount = Hours.hoursBetween(startDate, endDate).getHours

      // Download one file per hour and put in Spark Dataset
      import spark.implicits._
//      var pageviews = spark.emptyDataset[String]

      (0 until hoursCount).foreach{i =>
        val time = startDate.plusHours(i)
        val (year, m, d, h) = (time.getYear, time.getMonthOfYear, time.getDayOfMonth, time.getHourOfDay)
        // Add '0' if month/day/hour < 10 to make sure file name is correct
        val month = if(m < 10) s"0$m" else m.toString
        val day = if(d < 10) s"0$d" else d.toString
        val hour = if(h < 10) s"0$h" else h.toString
        val filename = s"pageviews-$year$month$day-${hour}0000.gz"
        val file = buildPath(DATA_PATH +: s"pageviews-$year$month$day-${hour}0000.gz" +: Nil)

        // Download the file when it is unavailable
        val FS = fs.FileSystem.get(sc.hadoopConfiguration)
        if(FS.exists(new fs.Path(file))) {
          println(s"$file exists")
//          pageviews = pageviews.union(spark.read.textFile(file))
        }
        else {
          println(s"$file doesn't exists, start downloading")
          fileDownloader(PAGEVIEWS+s"$year/$year-$month/$filename")
//          pageviews = pageviews.union(spark.read.textFile(fileDownloader(PAGEVIEWS+s"$year/$year-$month/$file")))
        }

      }
      val pageviews = spark.read.textFile(DATA_PATH).filter()
      /** TODO: Algorithm part for page views */
//      println(pageviews.count())
//      pageviews.show()
      true
      }
    println("Finish of Datadog")
    }

//
//    /** Test part */
//    val blackListPath = getClass.getResource("src/main/resources/blacklist_domains_and_pages").getPath
//    println(blackListPath)


}
