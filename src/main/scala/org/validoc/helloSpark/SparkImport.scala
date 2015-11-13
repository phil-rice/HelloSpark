package org.validoc.helloSpark

import java.text.SimpleDateFormat
import scala.io.Source
import play.api.libs.json._

object SparkImport extends UsingDatabase with UsingPostgres with TimeOperations with LocalSparks {
  //  def file = "src/main/resources/Cif.json"
  def file = "src/main/resources/CIF_ALL_FULL_DAILY.json"

  val mapFnLong: ((JsLookupResult, Long)) => MasterSlaveData = { case (j, l) => mapFn(j, l.toInt) }
  val mapFn: ((JsLookupResult, Int)) => MasterSlaveData = {
    case (json, serviceId) =>
      val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
      def s(j: JsLookupResult) = j.asOpt[String].getOrElse(null)
      def d(j: JsLookupResult) = new java.sql.Date(dateFormatter.parse(s(j)).getTime)
      val schedule = json \ "schedule_segment"
      val trainUid = s(json \ "CIF_train_uid")
      val trainCategory = s(schedule \ "CIF_train_category")
      val trainServiceCode = s(schedule \ "CIF_train_service_code")
      val scheduleDayRuns = s(json \ "schedule_days_runs")
      val scheduleStartDate = d(json \ "schedule_start_date")
      val scheduleEndDate = d(json \ "schedule_end_date")

      val serviceSegmentData = (schedule \ "schedule_location").asOpt[Seq[JsObject]] match {
        case Some(list) => list.map { json =>
          val tiploc = (json \ "tiploc_code").as[String]
          val departure = s(json \ "departure")
          val arrival = s(json \ "arrival")
          List(serviceId, tiploc, departure, arrival)
        }
        case None => List()
      }

      new MasterSlaveData(List(serviceId, trainUid, trainCategory, trainServiceCode, scheduleDayRuns, scheduleStartDate, scheduleEndDate), serviceSegmentData)
  }

  def main1(args: Array[String]): Unit = {
    val chunkSize = 50000
    val rawLines = Source.fromFile(file).getLines()
    val scheduleLines = rawLines.filter(_.contains("JsonScheduleV1")).map(Json.parse(_) \ "JsonScheduleV1")

    val scheduleData = scheduleLines.zipWithIndex.map { mapFn }

    val masterSlaveDefn = new MasterSlaveDefn(
      "service_for_blog",
      List("id", "uid", "train_category", "train_service_code", "scheduleDayRuns", "schedule_start_date", "schedule_end_date"),
      "service_segment_for_blob",
      List("service_id", "tiploc", "departure", "arrival"))

    withDatasource { implicit dataSource => masterSlaveInsert(masterSlaveDefn, scheduleData) }
  }

  def main(args: Array[String]): Unit = {
    val chunkSize = 50000
    time("spark", withSparkAndFile(file) {
      implicit sc =>
        rawLines =>
          val scheduleLines = rawLines.filter(_.contains("JsonScheduleV1")).map(Json.parse(_) \ "JsonScheduleV1")

          val scheduleData = scheduleLines.zipWithIndex.map { mapFnLong }

          val masterSlaveDefn = new MasterSlaveDefn(
            "service_for_blog",
            List("id", "uid", "train_category", "train_service_code", "scheduleDayRuns", "schedule_start_date", "schedule_end_date"),
            "service_segment_for_blob",
            List("service_id", "tiploc", "departure", "arrival"))
          println("Partitions: " + scheduleData.partitions.size)
          scheduleData.foreachPartition { chunk =>
            withDatasource { implicit ds => masterSlaveInsert(masterSlaveDefn, chunk, chunkSize) }
          }
    })
  }
}