package de.sparkproject

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {

    /*
    val spark = SparkSession.builder()
      .appName("de-spark-project")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("header", value = true)
      .option("inferSchema", true)
      .csv("data/AAPL.csv")

    // DSL
    df.show()
    df.printSchema()

    /*
    df.select("Date", "Open", "Close").show()
    import spark.implicits._
    df.select(col("Date"), $"Open", df("Close")).show()

    val column = df("Open")
    val newColumn = (column + (2.0)).as("OpenIncreasedBy2")
    val columnString = column.cast(StringType).as("OpenAsString")

    val litColumn = lit(2.0)
    val concatColumn = concat(columnString, lit("Hello World"))

    df.select(column, newColumn, columnString, concatColumn)
      .show(truncate = false)

    val timestampFromExpr = expr("cast(current_timestamp() as string) as timestampFromExpr")
    val timestampFromFunctions = current_timestamp().cast(StringType).as("timestampFromFunctions")

    df.select(timestampFromExpr, timestampFromFunctions).show()

    df.selectExpr("cast(Date as string)", "Open + 1.0", "current_timestamp()").show()

    df.createTempView("df")
    spark.sql("select * from df").show()
    */

    val renameColumns = List(
      col("Date").as("date"),
      col("Open").as("open"),
      col("High").as("high"),
      col("Low").as("low"),
      col("Close").as("close"),
      col("Adj Close").as("adjClose"),
      col("Volume").as("volume")
    )

    // df.select(df.columns.map(c => col(c).as(c.toLowerCase)): _*).show()
    val stockData = df.select(renameColumns: _*)
      .withColumn("diff", col("close") - col("open"))
      .filter(col("close") > col("open") * 1.1)

    stockData.show()

    import spark.implicits._
    stockData
      .groupBy(year($"date").as("year"))
      .agg(max($"close"), avg($"close"))
      .sort($"year".desc)
      .show()

    val highestClosingPricesPerYearData: DataFrame = highestClosingPricesPerYear(stockData)
    highestClosingPricesPerYearData.show()


     */
    class Box[T](value: T) {
      def get: T = value
    }

    val intBox = new Box[Int](42)
    val stringBox = new Box[String]("Scala")

    println(intBox.get)    // Output: 42
    println(stringBox.get) // Output: Scala


  }

  def highestClosingPricesPerYear(df: Dataset[Row]): DataFrame = {
    import df.sparkSession.implicits._
    val window = Window.partitionBy(year($"date").as("year")).orderBy($"close".desc)
    df
      .withColumn("rank", row_number().over(window))
      .filter($"rank" === 1)
      .drop("rank")
      .sort($"close".desc)

  }

  def add(x:Int, y:Int): Int = x + y

}
