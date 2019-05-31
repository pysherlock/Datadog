package DataDog

import sys.process._
import java.net.URL

import scala.io.Source
import java.net.URL
import java.io.{File, FileOutputStream, FileWriter, PrintWriter}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.hadoop.fs
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Hours}
import DatadogEncoders._
import SystemConfig._
import javafx.print.Printer
import org.apache.hadoop.fs.Path


object DataDog extends TParam with TFileUtils {

  lazy val conf = new SparkConf()
  lazy val spark = SparkSession.builder.config(conf).getOrCreate()
  lazy val sc = spark.sparkContext
  /** Link of pageviews */
  final val PAGEVIEWS = "https://dumps.wikimedia.org/other/pageviews/"
  /** Path of download files */
  lazy val DATA_PATH = Params.getOrElse(DATA, s"file:///${System.getProperty("user.dir")}")
  lazy val BLACK_LIST_PATH = Params.getOrElse(BLACK_LIST, s"file:///${System.getProperty("user.dir")}")

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

      // Read the blacklist
      val blackList = spark.read.textFile(BLACK_LIST_PATH)
        .map(l => PageViews(l).getDomainPage)
        .distinct().collect().toSet

      // Do the analyze per hour
      (0 until hoursCount).foreach{i =>
        val time = startDate.plusHours(i)
        val (year, m, d, h) = (time.getYear, time.getMonthOfYear, time.getDayOfMonth, time.getHourOfDay)
        // Add '0' if month/day/hour < 10 to make sure file name is correct
        val month = if(m < 10) s"0$m" else m.toString
        val day = if(d < 10) s"0$d" else d.toString
        val hour = if(h < 10) s"0$h" else h.toString
        val filename = s"pageviews-$year$month$day-${hour}0000.gz"
        val file = buildPath(DATA_PATH +: s"pageviews-$year$month$day-${hour}0000.gz" +: Nil)
        val resultPath = buildPath(DATA_PATH +: "result" +: s"pageviews-result-$year$month$day-${hour}0000" +: Nil)

        val FS = fs.FileSystem.get(sc.hadoopConfiguration)
        // Do the analyze if necessary
        if(!FS.exists(new Path(resultPath))) {
          def pageViewsAnalyzer(input: String): Array[PageViews] = {
            spark.read.textFile(input)
              .map(l => PageViews(l)).filter(p => !blackList.exists(b => p.getDomainPage.belongsTo(b)))
              .groupByKey(_.getDomainPage)
              .reduceGroups((a, b) => PageViews(a.domain, a.page, a.views+b.views, a.response_size+b.response_size))
              .map(_._2).groupByKey(_.domain).flatMapGroups((d, it) => it.toList.sortWith(_.views > _.views).slice(0, 25))
              .collect()
          }

          // Only Download the file when it is unavailable
          val pageViews = if(FS.exists(new fs.Path(file))) {
            println(s"$file exists")
            pageViewsAnalyzer(file)
          } else {
            println(s"$file doesn't exists, start downloading")
//            fileDownloader(PAGEVIEWS+s"$year/$year-$month/$filename")
            pageViewsAnalyzer(fileDownloader(PAGEVIEWS+s"$year/$year-$month/$file"))
          }
          // Save the result to local file
          val writer = new PrintWriter(new File(buildPath(DATA_PATH +: "result" +: s"pageviews-result-$year$month$day-${hour}0000" +: Nil)))
          pageViews.foreach(p => writer.write(p.toString))
          writer.close()
        }
      }
      true
    }
    println("Finish of Datadog")
  }
}
