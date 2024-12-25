package de.sparkproject

import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.sql.Date

class FirstTest extends AnyFunSuite {

  private val spark = SparkSession
    .builder()
    .appName("FirstTest")
    .master("local[*]")
    .getOrCreate()

  private val schema = StructType(
    Seq(
      StructField("date", DateType, nullable = true),
      StructField("open", DoubleType, nullable =  true),
      StructField("close", DoubleType, nullable =  true)
    )
  )


  test("add(2, 3) returns 5") {
    val testRows = Seq(
      Row(Date.valueOf("2022-01-12"), 1.0, 2.0),
      Row(Date.valueOf("2023-03-14"), 1.0, 2.0),
      Row(Date.valueOf("2023-11-10"), 1.0, 3.0)
    )
    val expected = Seq(
      Row(Date.valueOf("2022-01-12"), 1.0, 2.0),
      Row(Date.valueOf("2023-11-10"), 1.0, 3.0)
    )

    implicit val encoder: Encoder[Row] = Encoders.row(schema)
    val testDf = spark.createDataset(testRows)

    val resultList = Main.highestClosingPricesPerYear(testDf)
      .collect()

    resultList should contain theSameElementsAs expected
  }

}
