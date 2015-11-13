package org.validoc.helloSpark

import org.apache.commons.dbcp.BasicDataSource
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.sql.BatchUpdateException
import java.sql.PreparedStatement
import javax.sql.DataSource
import java.sql.Connection

object SizeMismatchException {
  def apply(lineNo: Int, columnNames: List[_], list: List[_]) = new SizeMismatchException(s"Line number [lineNo] should have ${columnNames.size} columns but has ${list.size} the data is $list")
}
class SizeMismatchException(message: String) extends Exception(message)
case class DataSourceDefn(url: String, userName: String, password: String, classDriveName: String = "org.postgresql.Driver", maxConnections: Integer = -1)

trait UsingPostgres {
  implicit lazy val defn: DataSourceDefn = DataSourceDefn(url = "jdbc:postgresql:orcats", userName = "postgres", password = "iwtbde")
}
class MasterSlaveDefn(val tableName1: String, val columnNames1: List[String],
                      val tableName2: String, val columnNames2: List[String]) extends Serializable
class MasterSlaveData(val masterData: List[Any], val slaveData: Seq[List[Any]])

trait UsingDatabase {
  implicit def defn: DataSourceDefn
  protected def createDataSource(implicit dsDefn: DataSourceDefn) = {
    val ds = new BasicDataSource()
    ds.setDriverClassName(dsDefn.classDriveName)
    ds.setUrl(dsDefn.url)
    ds.setUsername(dsDefn.userName)
    ds.setPassword(dsDefn.password)
    ds.setMaxActive(dsDefn.maxConnections)
    ds
  }
  protected def withDatasource[X](fn: (DataSource) => X)(implicit defn: DataSourceDefn) = {
    val ds = createDataSource
    try fn(ds) finally ds.close
  }

  protected def withConnection[X](fn: (Connection => X))(implicit ds: DataSource) = {
    val c = ds.getConnection
    try fn(c) finally c.close
  }

  protected def withPreparedStatement[X](sql: String, fn: (PreparedStatement) => X)(implicit ds: DataSource) = withConnection { connection =>
    val statement = connection.prepareStatement(sql)
    try fn(statement) finally statement.close
  }

  protected def dataToDatabase(tableName: String, columnNames: List[String], data: Iterator[List[Any]])(implicit ds: DataSource) = {
    val columnsWithCommas = columnNames.mkString(",")
    val questionMarks = columnNames.map(_ => "?").mkString(",")
    val sql = s"insert into $tableName ($columnsWithCommas) values ($questionMarks)"
    for ((list, lineNo) <- data.zipWithIndex)
      withPreparedStatement(sql, { implicit statement =>
        for ((value, index) <- list.zipWithIndex)
          statement.setObject(index + 1, value)
        statement.execute
      })
  }

  def withStatementForInsert(tableName: String, columnNames: List[String], chunkSize: Int = 10000)(statementFn: PreparedStatement => Unit)(implicit connection: Connection) = try {
    val columnsWithCommas = columnNames.mkString(",")
    val questionMarks = columnNames.map(_ => "?").mkString(",")
    val sql = s"insert into $tableName ($columnsWithCommas) values ($questionMarks)"
    val statement = connection.prepareStatement(sql)
    try statementFn(statement) finally statement.close
  } catch { case e: BatchUpdateException => e.printStackTrace(); throw e }

  def withStatementsForInsert(defn: MasterSlaveDefn)(
    statementFn: (PreparedStatement, PreparedStatement) => Unit)(implicit dataSource: DataSource) = {
    import defn._
    withConnection { implicit connection1 =>
      withStatementForInsert(tableName1, columnNames1) { statement1 =>
        withStatementForInsert(tableName2, columnNames2) { statement2 =>
          statementFn(statement1, statement2)
        }
      }
    }
  }

  def addToStatement(statement: PreparedStatement, list: List[Any]) = {
    for { (value, index) <- list.zipWithIndex }
      statement.setObject(index + 1, value)
    statement.addBatch()
  }

  def masterSlaveInsert(defn: MasterSlaveDefn, data: Iterator[MasterSlaveData], chunkSize: Int = 10000)(implicit ds: DataSource) =
    withStatementsForInsert(defn) { (master, slave) =>
      for (chunk <- data.sliding(chunkSize, chunkSize)) {
        for { masterSlaveData <- chunk } {
          addToStatement(master, masterSlaveData.masterData)
          for (s <- masterSlaveData.slaveData)
            addToStatement(slave, s)
        }
        master.executeBatch()
        slave.executeBatch()
      }
    }

  protected def batchDataToDatabase(tableName: String, columnNames: List[String], data: Iterator[List[Any]], chunkSize: Int = 10000)(implicit ds: DataSource) = {
    val columnsWithCommas = columnNames.mkString(",")
    val questionMarks = columnNames.map(_ => "?").mkString(",")
    val sql = s"insert into $tableName ($columnsWithCommas) values ($questionMarks)"
    withPreparedStatement(sql, implicit statement => {
      for (chunk <- data.sliding(chunkSize, chunkSize)) {
        for { list <- chunk } {
          for { (value, index) <- list.zipWithIndex }
            statement.setObject(index + 1, value)
          statement.addBatch()
        }
        statement.executeBatch()
      }
    })
  }

  protected def iteratorToDatabase(tableName: String, columnNames: List[String], data: Iterable[List[Any]], chunkSize: Int = 100, debug: Boolean = false)(implicit ds: DataSource) = {
    var i = 0
    for (slice <- data.sliding(chunkSize, chunkSize)) {
      dataToDatabase(tableName, columnNames, slice.iterator)
      if (debug) {
        i += 1
        print('.')
        if (i % 100 == 0) println
      }
    }
  }
  protected def rddToDatabase[X](rdd: RDD[X], tableName: String, columnNames: List[String])(lineMapper: (X) => List[Any])(implicit sc: SparkContext, defn: DataSourceDefn) = {
    println("in rdd to database")
    rdd.foreachPartition { lines =>
      val data = lines.map(lineMapper)
      withDatasource {
        implicit ds => dataToDatabase(tableName, columnNames, data)
      }
    }
  }
}

