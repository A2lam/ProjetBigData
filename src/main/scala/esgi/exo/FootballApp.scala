package esgi.exo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.udf
import org.apache.spark

import java.text.SimpleDateFormat
import java.util.Date

object FootballApp {
  // Function that holds int casting
  def hold_integers(strInt: String): Integer = {
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
    val df_matches = spark
      .read
      .option("header", value = true)
      .option("inferSchema", "true")
      .csv("input/df_matches.csv")

    // Turning it into UDF
    val extract_integer = udf(this.hold_integers _)

    // Selecting wanted columns and casting
    val col_score_france = col("score_france")
    val col_score_adversaire = col("score_adversaire")
    val col_penalty_france = col("penalty_france")
    val col_penalty_adversaire = col("penalty_adversaire")
    val col_date = col("date")

    val df_casted = df_matches
      .select("X4", "X6", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")
      .withColumn("score_france_casted", extract_integer(col_score_france))
      .withColumn("score_adversaire_casted", extract_integer(col_score_adversaire))
      .withColumn("penalty_france_casted", extract_integer(col_penalty_france))
      .withColumn("penalty_adversaire_casted", extract_integer(col_penalty_adversaire))
      .withColumn("date_casted", to_date(col_date,"yyyy-MM-dd"))
      .drop("score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

    // Renaming some columns
    val df_matches_france = df_casted
      .withColumnRenamed("X4", "match")
      .withColumnRenamed("X6", "competition")
      .withColumnRenamed("score_france_casted", "score_france")
      .withColumnRenamed("score_adversaire_casted", "score_adversaire")
      .withColumnRenamed("penalty_france_casted", "penalty_france")
      .withColumnRenamed("penalty_adversaire_casted", "penalty_adversaire")
      .withColumnRenamed("date_casted", "date")

    // Keeping only matches above 1980
    val df_matches_france_after_1980 = df_matches_france.filter(col_date >= "1980-03-01")

    df_matches_france_after_1980.printSchema()
    df_matches_france_after_1980.show(3)

    // Stopping the spark session
    spark.stop()
  }
}
