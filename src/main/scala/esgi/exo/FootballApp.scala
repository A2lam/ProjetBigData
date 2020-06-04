package esgi.exo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.udf

object FootballApp {
  // Function that holds int casting
  def convertToInt(strInt: String): Integer = {
    if (strInt == "NA")
      return 0

    Integer.valueOf(strInt)
  }

  // Main program
  def main(args: Array[String]) {
    // Creating spark session
    val spark = SparkSession
      .builder
      .appName("Program")
      .config("spark.master", "local")
      .getOrCreate()

    // Reading the the csv input file into dataFrame
    val dfMatches = spark
      .read
      .option("header", value = true)
      .option("inferSchema", "true")
      .csv("input/df_matches.csv")

    // Turning it into UDF
    val extractInteger = udf(this.convertToInt _)

    // Selecting wanted columns and casting
    val colScoreFrance = col("score_france")
    val colScoreAdversaire = col("score_adversaire")
    val colPenaltyFrance = col("penalty_france")
    val colPenaltyAdversaire = col("penalty_adversaire")
    val colDate = col("date")

    val dfCasted = dfMatches
      .select("X4", "X6", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")
      .withColumn("score_france_casted", extractInteger(colScoreFrance))
      .withColumn("score_adversaire_casted", extractInteger(colScoreAdversaire))
      .withColumn("penalty_france_casted", extractInteger(colPenaltyFrance))
      .withColumn("penalty_adversaire_casted", extractInteger(colPenaltyAdversaire))
      .withColumn("date_casted", to_date(colDate,"yyyy-MM-dd"))
      .drop("score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

    // Renaming some columns
    val dfMatchesFrance = dfCasted
      .withColumnRenamed("X4", "match")
      .withColumnRenamed("X6", "competition")
      .withColumnRenamed("score_france_casted", "score_france")
      .withColumnRenamed("score_adversaire_casted", "score_adversaire")
      .withColumnRenamed("penalty_france_casted", "penalty_france")
      .withColumnRenamed("penalty_adversaire_casted", "penalty_adversaire")
      .withColumnRenamed("date_casted", "date")

    // Keeping only matches above 1980
    val dfMatchesFranceAfter1980 = dfMatchesFrance.filter(colDate >= "1980-03-01")

    dfMatchesFranceAfter1980.printSchema()
    dfMatchesFranceAfter1980.show(3)

    // Stopping the spark session
    spark.stop()
  }
}
