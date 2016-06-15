package com.datastax.spark.connector

import com.datastax.driver.core.LocalDate
import org.joda.time.{LocalDate => JodaLocalDate}

import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class GettableDataSpec extends FlatSpec with Matchers {

  "GettableData" should "convert Driver LocalDates  to Joda LocalDate" in {
    Random.setSeed(10)
    val trials = 10000

    val dates = for (t <- 1 to trials) yield {
      val year = Random.nextInt(2000)
      val month = Random.nextInt(12) + 1
      val day = Random.nextInt(28) + 1
      val localDate = (LocalDate.fromYearMonthDay(year, month, day))
      val jodaLocalDate = GettableData.convert(localDate).asInstanceOf[JodaLocalDate]

      val ld = (localDate.getYear, localDate.getMonth, localDate.getDay)
      val jd = (jodaLocalDate.getYear, jodaLocalDate.getMonthOfYear, jodaLocalDate.getDayOfMonth)

      ld should be (jd)
    }







  }

}
