package com.datastax.spark.connector.rdd.typeTests

import java.sql.Date
import java.util.TimeZone

import com.datastax.driver.core.Row
import com.datastax.driver.core.LocalDate

abstract class DateTypeTest extends AbstractTypeTest[Date, LocalDate]{

  val defaultTimeZone = java.util.TimeZone.getDefault

  override protected val typeName: String = "date"

  override protected val typeData: Seq[Date] = Seq("2015-05-01", "2015-05-10", "2015-05-20","1950-03-05")
  override protected val addData: Seq[Date] = Seq("2011-05-01", "2011-05-10", "2011-05-20","1950-01-01")

  override def getDriverColumn(row: Row, colName: String): Date = {
    val ld = row.getDate(colName)
    Date.valueOf(ld.toString)
  }

  implicit def strToDate(str: String) : Date = java.sql.Date.valueOf(str)

  val dateRegx = """(\d\d\d\d)-(\d\d)-(\d\d)""".r

  override def convertToDriverInsertable(testValue: Date): LocalDate = {
    testValue.toString match { case dateRegx(year, month, day) =>
        LocalDate.fromYearMonthDay(year.toInt, month.toInt, day.toInt)
    }
  }
}

class PSTDateTypeTest extends DateTypeTest {
  override def beforeAll(): Unit = {
    super.beforeAll()
    TimeZone.setDefault(TimeZone.getTimeZone("PST"))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TimeZone.setDefault(defaultTimeZone)
  }
}

class CETDateTypeTest extends DateTypeTest {
  override def beforeAll(): Unit = {
    super.beforeAll()
    TimeZone.setDefault(TimeZone.getTimeZone("CET"))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TimeZone.setDefault(defaultTimeZone)
  }
}
