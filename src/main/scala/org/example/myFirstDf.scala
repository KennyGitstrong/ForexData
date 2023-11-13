package org.example
import org.apache.spark.sql.SparkSession

object myFirstDf {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Write To Csv")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.{SparkSession, functions}

    val Employee = Seq("JobRole", "Position", "Salary")
    val data = Seq(
      ("Data Engineer", "Junior", 45000),
      ("Cloud Engineer", "Lead", 150000),
      ("Devops Engineer", "Senior", 105000),
      ("Full Stack dev", "Senior", 95000)
    )
    val dataframe1 = data.toDF(Employee: _*)
    dataframe1.show()

    val filteredDF1 = dataframe1.filter($"Salary" > 85000)
    filteredDF1.show()

    val filteredDF2 = dataframe1.filter($"Salary" < 95000)
    filteredDF2.show()

    val nameColumn = dataframe1.select("JobRole")
    nameColumn.show()

    val AvgAge = dataframe1.agg(functions.avg("Salary"))
    AvgAge.show()

    val sortedDF1 = dataframe1.orderBy($"JobRole".desc)
    sortedDF1.show()

    val sortedDF2 = dataframe1.orderBy($"JobRole".asc)
    sortedDF2.show()

    val groupDF = dataframe1.groupBy($"Position").count()
    groupDF.show()

    val additionalDF = Seq(
      ("University Of Johannesburg", "Devops Engineer"),
      ("HyperionDev", "Data Engineer"),
      ("Hack Reactor", "Full Stack dev"),
      ("University Of Pretoria", "Cloud Engineer")
    ).toDF("Education", "JobRole")

    val joinedDF = dataframe1.join(additionalDF, "JobRole")
    joinedDF.show()

    joinedDF.write
      .mode("overwrite")
      .option("header", "true")
      .csv("file:/C:/Users/Kenny Makenzo/Desktop/myfirstCsv.csv")

    spark.stop()
  }
}
