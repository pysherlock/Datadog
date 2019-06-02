package DataDog

import sys.process._

import java.net.URL
import java.io.{File, PrintWriter}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.hadoop.fs
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Hours}
import SystemConfig._
import org.apache.hadoop.fs.Path


object DataDog extends TParam with TFileUtils {

  lazy val conf = new SparkConf()
  lazy val spark = SparkSession.builder.config(conf).getOrCreate()
  lazy val sc = spark.sparkContext
  lazy val TIMEOUT = 60L
  /** Link of pageviews */
  final val PAGEVIEWS = "https://dumps.wikimedia.org/other/pageviews/"
  /** Default Link of the file of Black list */
  final val BLACK_LIST_LINK = "https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages"
  /** Path of download files */
  lazy val DATA_PATH = Params.getOrElse(DATA, s"${System.getProperty("user.dir")}/data")
  /** Path of results */
  lazy val RESULT_PATH = Params.getOrElse(RESULT, s"${System.getProperty("user.dir")}/result")
  /** File name of blacklist */
  final val BLACK_LIST_FILE = "blacklist_domains_and_pages"
  /** Path of the blacklist file */
  lazy val BLACK_LIST_PATH = Params.getOrElse(BLACK_LIST, s"${System.getProperty("user.dir")}")

  /** Download file for a web url
    *
    * @param dir: The directory path for keeping downloaded file
    * @param url: The web url where to download the file
    * @return The path of downloaded file
    */
  def fileDownloader(dir: String, url: String): String = synchronized {
    try {
      val filename = url.split("/").last
      println(filename)
      val path = buildPath(dir +: filename +: Nil)
      println(path)
      new URL(url) #> new File(path) !!;
      path
    } catch {
      case t: Throwable => println("Fail to Download the File")
        t.printStackTrace(); ""
    }
  }

  /** Entry of the application */
  def main(args: Array[String]): Unit = synchronized {

    SystemConfig.parseCmdLine(args).foreach { _ =>
      val startDate = DateTime.parse(Params.getOrElse(START_DATE, ""), DateTimeFormat.forPattern("yyyy-MM-dd:HH"))
      val endDate = DateTime.parse(Params.getOrElse(END_DATE, ""), DateTimeFormat.forPattern("yyyy-MM-dd:HH"))
      val hoursCount = Hours.hoursBetween(startDate, endDate).getHours

      val FS = fs.FileSystem.get(sc.hadoopConfiguration)

      import spark.implicits._
      // Read the blacklist, download when it is not available
      val blackList = if(FS.exists(new Path(s"$BLACK_LIST_PATH/$BLACK_LIST_FILE"))) {
        sc.broadcast(spark.read.textFile(s"$BLACK_LIST_PATH/$BLACK_LIST_FILE")
          .map(l => PageViews(l).getDomainPage)
          .distinct().collect().toSet)
      } else {
        val bl = fileDownloader(BLACK_LIST_PATH, BLACK_LIST_LINK)
        sc.broadcast(spark.read.textFile(bl)
          .map(l => PageViews(l).getDomainPage)
          .distinct().collect().toSet)
      }

      // Do the analyze per hour
      (0 until hoursCount).foreach { i =>
        val time = startDate.plusHours(i)
        val (year, m, d, h) = (time.getYear, time.getMonthOfYear, time.getDayOfMonth, time.getHourOfDay)
        // Add '0' if month/day/hour < 10 to make sure file name is correct
        val month = if (m < 10) s"0$m" else m.toString
        val day = if (d < 10) s"0$d" else d.toString
        val hour = if (h < 10) s"0$h" else h.toString
        val filename = s"pageviews-$year$month$day-${hour}0000.gz"
        val file = buildPath(DATA_PATH +: filename +: Nil)
        val resultPath = buildPath(RESULT_PATH +: s"pageviews-result-$year$month$day-${hour}0000" +: Nil)
        val FS = fs.FileSystem.get(sc.hadoopConfiguration) // Avoid task non-serializable problem

        // Do the analyze if necessary
        if (!FS.exists(new Path(resultPath))) {

          def pageViewsAnalyzer(input: String): Array[PageViews] = {
            spark.read.textFile(input)
              .map(l => PageViews(l)).filter(p => !blackList.value.contains(p.getDomainPage))
              .groupByKey(_.getDomainPage)
              .reduceGroups((a, b) => PageViews(a.domain, a.page, a.views + b.views, a.response_size + b.response_size)).map(_._2)
              .groupByKey(_.domain).flatMapGroups((d, it) => it.toList.sortWith(_.views > _.views).slice(0, 25))
              .filter(_.getDomainPageName.nonEmpty).collect()
          }

          // Only Download the file when it is unavailable
          val pageViews = if (FS.exists(new fs.Path(file))) {
            println(s"$file exists")
            pageViewsAnalyzer(file)

          } else {
            println(s"$file doesn't exists, start downloading")
            pageViewsAnalyzer(fileDownloader(DATA_PATH, PAGEVIEWS + s"$year/$year-$month/$filename"))
          }
          // Save the result to local file
          val writer = new PrintWriter(new File(buildPath(RESULT_PATH +: s"pageviews-result-$year$month$day-${hour}0000" +: Nil)))
          pageViews.groupBy(_.domain).toArray.sortBy(_._1).flatMap(_._2).foreach(p => writer.write(p.toString))
          writer.close()
        }
      }
      blackList.destroy()
    }
    println("Finish of Datadog")
  }
}
