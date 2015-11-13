package org.validoc.helloSpark

//import play.api.libs.json.Json

import scala.io.Source
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import play.api.libs.json.Json
import play.api.libs.json.JsValue
import java.text.SimpleDateFormat
import play.api.libs.json.JsLookup
import play.api.libs.json.JsLookupResult
import java.sql.PreparedStatement
import java.sql.BatchUpdateException
import javax.sql.DataSource
import java.sql.Connection
import org.apache.commons.dbcp.BasicDataSource

trait SparkOps {
  def sparkTitle: String
  def sparkUrl: String

  def withSpark[X](fn: SparkContext => X) = {
    val conf = new SparkConf().setAppName(sparkTitle).setMaster(sparkUrl)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    fn(sc)
  }

  def withFile[X](fileName: String)(fn: SparkContext => RDD[String] => X)(implicit sc: SparkContext) = fn(sc)(sc.textFile(fileName))

  def withSparkAndFile[X](fileName: String)(fn: SparkContext => RDD[String] => X) =
    withSpark { implicit sc => withFile(fileName)(fn) }
}

trait LocalSparks extends SparkOps {
  def sparkUrl = "local[6]"
  def sparkTitle = getClass.getSimpleName
}

object HelloSpark extends LocalSparks with UsingDatabase with UsingPostgres {
  def file = "src/main/scala/org/validoc/helloSpark/HelloSpark.scala"
  def jsonFile = "src/main/resources/Cif.json"

  def firstMain(args: Array[String]): Unit = {
    val lines = Source.fromFile(file).getLines().
      zipWithIndex.map { case (line, index) => f"$index%2d $line" }
    lines.foreach { println }
  }
  def secondMain(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HelloSpark").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(file).
      zipWithIndex.map { case (line, index) => f"$index%2d $line" }
    lines.foreach { println }
  }
  def thirdMain(args: Array[String]): Unit = {
    withSparkAndFile(file) { sparkContext =>
      rawLines =>
        val lines = rawLines.zipWithIndex.map { case (line, index) => f"$index%2d $line" }
        lines.foreach { println }
    }
  }

  def main(args: Array[String]): Unit = {
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    thirdMain(args)
    //    withSparkAndFile(jsonFile) { implicit sparkContext =>
    //      rawLines =>
    //
    //        val scheduleLines = rawLines.filter(_.contains("JsonScheduleV1")).map(Json.parse(_) \ "JsonScheduleV1")
    //        val dataForDatabase = scheduleLines.zipWithIndex().map { jsonAndId =>
    //          val (json, id) = jsonAndId
    //          def s(j: JsLookupResult) = j.asOpt[String].getOrElse(null)
    //          def d(j: JsLookupResult) = new java.sql.Date(dateFormatter.parse(s(j)).getTime)
    //          val schedule = json \ "schedule_segment"
    //          val trainUid = s(json \ "CIF_train_uid")
    //          val trainCategory = s(schedule \ "CIF_train_category")
    //          val trainServiceCode = s(schedule \ "CIF_train_service_code")
    //          val scheduleDayRuns = s(json \ "schedule_days_runs")
    //          val scheduleStartDate = d(json \ "schedule_start_date")
    //          val scheduleEndDate = d(json \ "schedule_end_date")
    //          List(id, Json.stringify(json.get), trainUid, trainCategory, trainServiceCode, scheduleDayRuns, scheduleStartDate, scheduleEndDate)
    //        }
    //        val columnNames = List("id", "json", "uid", "train_category", "train_service_code", "scheduleDayRuns", "schedule_start_date", "schedule_end_date")
    //        rddToDatabase(dataForDatabase, "serviceForBlog", columnNames)(_)
    //    }
  }

}