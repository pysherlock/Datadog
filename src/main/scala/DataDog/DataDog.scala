package DataDog

import scala.io.Source
import scala.reflect.io.File
import java.net.URL
import java.io.{File, FileOutputStream, FileWriter}
import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.hadoop.fs
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Hours}
import DatadogEncoders._
import org.apache.hadoop.fs.Path


object DataDog extends TParam {

  lazy val conf = new SparkConf()
  lazy val spark = SparkSession.builder.config(conf).getOrCreate()
  lazy val sc = spark.sparkContext
  /** Path of the BLACK LIST File*/
  val BLACK_LIST = "BLACK_LIST_PATH"
  /** Link of pageviews */
  final val PAGEVIEWS = "https://dumps.wikimedia.org/other/pageviews/"
  /** Path of download files*/
  final val DATA_PATH = getClass.getClassLoader.getResource("data").getPath+"/"

  def fileDownloader(url: String): String = {
    try {
      val filename = url.split("/").last
      val src = new URL(url).openConnection().getInputStream
      val out = new FileOutputStream(DATA_PATH+filename)
      var buffer = 0
      while (buffer != -1) {
        buffer = src.read()
        out.write(buffer)
      }
      out.close(); src.close(); DATA_PATH+filename
    } catch {
      case e: Throwable => println("Fail to Download the File")
        e.printStackTrace()
        ""
    }
  }

  /** Entry of the application */
  def main(args: Array[String]): Unit = {

    ParseCmdLine.parse(args).map{param =>
      val startDate = DateTime.parse(param.getOrElse(START_DATE, ""), DateTimeFormat.forPattern("yyyy-MM-dd:HH"))
      val endDate = DateTime.parse(param.getOrElse(END_DATE, ""), DateTimeFormat.forPattern("yyyy-MM-dd:HH"))
      val hoursCount = Hours.hoursBetween(startDate, endDate).getHours

      // Download one file per hour and put in Spark Dataset
//      import spark.implicits._
//      val pageviews = spark.emptyDataset[String]

      (0 until hoursCount).foreach{i =>
        val time = startDate.plusHours(i)
        val (year, m, d, h) = (time.getYear, time.getMonthOfYear, time.getDayOfMonth, time.getHourOfDay)
        val month = if(m < 10) s"0$m" else m.toString
        val day = if(d < 10) s"0$d" else d.toString
        val hour = if(h < 10) s"0$h" else h.toString
        val file = DATA_PATH+s"pageviews-$year$month$day-${hour}0000.gz"

        // Download the file when it is unavailable
        val FS = fs.FileSystem.get(sc.hadoopConfiguration)
        println(file)
        println(new Path(file).getName)
        if(FS.exists(new fs.Path(file))) {
          println(s"$file exists")
//          pageviews.union(spark.read.textFile(DATA_PATH+file))
        }
        else {
          println(s"$file doesn't exists, start downloading")
//          pageviews.union(spark.read.textFile(fileDownloader(PAGEVIEWS+s"$year/$year-$month/$file")))
        }

      }
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
//
////    import spark.implicits._
////    spark.createDataset(Seq(1,2,3)).show()
////    val blackList = spark.read.textFile("IdeaProjects/Datadog/src/main/resources/blacklist_domains_and_pages")
//
//    println("Datadog")
//    println("Test of fileDownloader")
//    fileDownloader(PAGEVIEWS+"2019/2019-05/pageviews-20190501-000000.gz")


}
