package org.validoc.helloSpark

import java.io.FileInputStream
import java.io.File
import java.io.BufferedInputStream
import java.io.BufferedReader
import java.io.FileReader
import scala.io.Source
import play.api.libs.json.Json
import play.api.libs.json.JsLookupResult
import java.text.SimpleDateFormat

trait UsingFiles {
  def withFile1(filename: String)(fn: String => Unit) {
    val fileReader = new FileReader(filename);
    try {
      val bufferedReader = new BufferedReader(fileReader);
      var line = bufferedReader.readLine
      while (line != null) {
        fn(line)
        line = bufferedReader.readLine
      }
    } finally { fileReader.close }
  }
  def withFile[X](filename: String)(fn: String => Unit) {
    val r = Source.fromFile(filename)
    try r.getLines().foreach(fn) finally { r.close() }
  }
  def mapFile[X](filename: String)(mapFn: String => X)(fn: X => Unit) =
    withFile(filename) { line => fn(mapFn(line)) }

}

trait TimeOperations {
  def time[X](title: String, x: => X) = {
    val newStart = System.currentTimeMillis()
    val result = x
    val duration = (System.currentTimeMillis() - newStart) / 1000.0
    println(f"title took ${duration}s")
    duration
  }
}

object SimpleImport extends UsingDatabase with UsingPostgres with TimeOperations {
  //  def file = "src/main/resources/Cif.json"
  def file = "src/main/resources/CIF_ALL_FULL_DAILY.json"

  def main(args: Array[String]): Unit = {
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    val rawLines = Source.fromFile(file).getLines()
    val scheduleLines = rawLines.filter(_.contains("JsonScheduleV1")).map(Json.parse(_) \ "JsonScheduleV1")
    val scheduleData = scheduleLines.zipWithIndex.map {
      case (json, id) =>
        def s(j: JsLookupResult) = j.asOpt[String].getOrElse(null)
        def d(j: JsLookupResult) = new java.sql.Date(dateFormatter.parse(s(j)).getTime)
        val schedule = json \ "schedule_segment"
        val trainUid = s(json \ "CIF_train_uid")
        val trainCategory = s(schedule \ "CIF_train_category")
        val trainServiceCode = s(schedule \ "CIF_train_service_code")
        val scheduleDayRuns = s(json \ "schedule_days_runs")
        val scheduleStartDate = d(json \ "schedule_start_date")
        val scheduleEndDate = d(json \ "schedule_end_date")
        List(id, trainUid, trainCategory, trainServiceCode, scheduleDayRuns, scheduleStartDate, scheduleEndDate)
    }

    time("just reading the data", scheduleData.foreach(x => x))
    val columnNames = List("id", "uid", "train_category", "train_service_code", "scheduleDayRuns", "schedule_start_date", "schedule_end_date")
    withDatasource { implicit dataSource =>
      time("dataToDatabase", batchDataToDatabase("serviceForBlog", columnNames, scheduleData))
    }

  }
}