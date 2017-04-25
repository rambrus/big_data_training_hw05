package com.epam.training.spark.sql

import org.apache.spark.sql.catalyst.expressions.DayOfMonth
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object Homework {
  val DELIMITER = ";"
  val RAW_BUDAPEST_DATA = "data/budapest_daily_1901-2010.csv"
  val OUTPUT_DUR = "output"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("EPAM BigData training Spark SQL homework")
      .setIfMissing("spark.master", "local[2]")
      .setIfMissing("spark.sql.shuffle.partitions", "10")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    processData(sqlContext)

    sc.stop()

  }

  def processData(sqlContext: HiveContext): Unit = {

    /**
      * Task 1
      * Read csv data with DataSource API from provided file
      * Hint: schema is in the Constants object
      */
    val climateDataFrame: DataFrame = readCsvData(sqlContext, Homework.RAW_BUDAPEST_DATA)

    /**
      * Task 2
      * Find errors or missing values in the data
      * Hint: try to use udf for the null check
      */
    val errors: Array[Row] = findErrors(climateDataFrame)
    println(errors)

    /**
      * Task 3
      * List average temperature for a given day in every year
      */
    val averageTemeperatureDataFrame: DataFrame = averageTemperature(climateDataFrame, 1, 2)

    /**
      * Task 4
      * Predict temperature based on mean temperature for every year including 1 day before and after
      * For the given month 1 and day 2 (2nd January) include days 1st January and 3rd January in the calculation
      * Hint: if the dataframe contains a single row with a single double value you can get the double like this "df.first().getDouble(0)"
      */
    val predictedTemperature: Double = predictTemperature(climateDataFrame, 1, 2)
    println(s"Predicted temperature: $predictedTemperature")

  }

  def readCsvData(sqlContext: HiveContext, rawDataPath: String): DataFrame = {
    sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(Constants.CLIMATE_TYPE).option("delimiter", ";").load(rawDataPath)
  }

  def findErrors(climateDataFrame: DataFrame): Array[Row] = {
    def isNull = udf((x: Any) => x == null)

    val columnErrors = Constants.CLIMATE_TYPE.fieldNames.map(name => sum(when(isNull(col(name)), 1).otherwise(0)))

    climateDataFrame.agg(columnErrors.head, columnErrors.tail: _*).collect()
  }

  def averageTemperature(climateDataFrame: DataFrame, monthNumber: Int, dayOfMonth: Int): DataFrame = {
    climateDataFrame.select("mean_temperature").where(month(col("observation_date")) === monthNumber and dayofmonth(col("observation_date")) === dayOfMonth)
  }

  def predictTemperature(climateDataFrame: DataFrame, monthNumber: Int, dayOfMonth: Int): Double = {
    def relevantObservation(monthNumber: Int, dayOfMonth: Int) = (month(col("observation_date")) ===  monthNumber) && (dayofmonth(col("observation_date")) between (dayOfMonth - 1, dayOfMonth + 1))

    climateDataFrame.where(relevantObservation(monthNumber, dayOfMonth)).agg(avg("mean_temperature")).first().getDouble(0)
  }
}


