package org.example

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MyForexData {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder
      .appName("MyForexData")
      .master("local[*]")
      .getOrCreate()

    // Define the financial pairs
    val TradingPairs = List("XAUUSD", "DOWJONES", "NASDAQ", "GBPUSD", "USDZAR")

    // Fetch data for each financial pair
    val dataFrames = TradingPairs.map(fetchFinancialData(spark, _))

    // Union all the data frames
    val joinedData = dataFrames.reduce(_ union _)

    // Show the data
    joinedData.show()

    // Calculate total and average for each financial pair
    val result = joinedData.groupBy("Pair")
      .agg(
        max("High").alias("MaxHigh"),
        min("Low").alias("MinLow"),
        avg("High").alias("AvgHigh"),
        avg("Low").alias("AvgLow")
      )

    // Show the result
    result.show()

    // Write to CSV
    result.write.mode("overwrite").csv("c:/users/Kenny Makenzo/desktop/financial_data.csv")

    // Write to Parquet
    result.write.mode("overwrite").parquet("c:/users/Kenny Makenzo/desktop/financial_data.parquet")

    // Stop the Spark session
    spark.stop()
  }

  def fetchFinancialData(spark: SparkSession, pair: String): DataFrame = {
    // For the sake of the example, creating a simple DataFrame with random data
    val schema = StructType(Seq(
      StructField("Pair", StringType, nullable = false),
      StructField("Date", TimestampType, nullable = false),  // Change to TimestampType
      StructField("High", DoubleType, nullable = false),
      StructField("Low", DoubleType, nullable = false)
    ))

    val data = (1 to 7).map(day => {
      val date = s"2023-11-0$day"
      val high = scala.util.Random.nextDouble() * 1000 + 1000 // Random high value between 1000 and 2000
      val low = scala.util.Random.nextDouble() * 1000 + 500  // Random low value between 500 and 1500
      Row(pair, java.sql.Timestamp.valueOf(date + " 00:00:00"), high, low)
    })

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }
}
