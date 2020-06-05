package esgi.exo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types.IntegerType

object FootballApp {
  // Function that holds int casting
  def convertToInt(strInt: String): Integer = {
    if (strInt == "NA")
      return 0

    Integer.valueOf(strInt)
  }

  // Function that holds home - away checking
  def homeAwayChecking(game: String): Boolean = {
    val homeTeam = game.split(" ")
    homeTeam(0) == "France"
  }

  // Main program
  def main(args: Array[String]) {
    // Creating spark session
    val spark = SparkSession
      .builder
      .appName("Program")
      .config("spark.master", "local")
      .getOrCreate()

    // Checking if the file path was submitted
    if (args.length == 0) {
      println("Veuillez soumettre le chemin du fichier en argument")

      // Stopping the spark session
      spark.stop()
    }

    /**
     * Part 1 : Cleaning data
     */
    // Reading the the csv input file into dataFrame
    val dfMatches = spark
      .read
      .option("header", value = true)
      .option("inferSchema", "true")
      .csv(args(0))

    // Declaring extract integer UDF
    val extractInteger = udf(this.convertToInt _)

    val colScoreFrance = col("score_france")
    val colScoreAdversaire = col("score_adversaire")
    val colPenaltyFrance = col("penalty_france")
    val colPenaltyAdversaire = col("penalty_adversaire")
    val colDate = col("date")

    // Selecting wanted columns and casting
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

    // Printing result df schema and some rows
    dfMatchesFranceAfter1980.printSchema()
    dfMatchesFranceAfter1980.show(100)

    /**
     * Part 2 : Statistics
     */
    // Declaring check home - away UDF
    val homeAwayCheck = udf(this.homeAwayChecking _)

    val colMatch = col("match")
    val colAdversaire = col("adversaire")
    val colHomeMatch = col("match_a_domicile")
    val colCompetition = col("competition")

    // Adding home - away checking column
    val dfMatchesFranceHA = dfMatchesFranceAfter1980.withColumn("match_a_domicile", homeAwayCheck(colMatch))

    // Calculating stats
    val dfStats = dfMatchesFranceHA
      .groupBy(colAdversaire)
      .agg(
        count(lit(1)).alias("total_match"),
        avg(colScoreFrance).alias("points_moyen_france"),
        avg(colScoreAdversaire).alias("points_moyen_adversaire"),
        (sum(colHomeMatch.cast(IntegerType)) * 100 / count(colHomeMatch)).alias("%_match_domicile"),
        sum(colCompetition.contains("Coupe du monde").cast(IntegerType)).alias("nb_match_CDM"),
        max(colPenaltyAdversaire).alias("plus_grand_nb_penalites_adversaire"),
        (sum(colPenaltyAdversaire) - sum(colPenaltyFrance)).alias("rapport_penalites_adversaire_france")
      )

    // Printing df schema and some rows of result and writing into parquet file
    dfStats.printSchema()
    dfStats.show(100)
    dfStats.write.mode("overwrite").parquet("data/stats.parquet")

    /**
     * Part 3 : Joining
     */
    // Reading the parquet file
    val dfStatsWritten = spark.read.parquet("data/stats.parquet")
    // Joining with the firs df
    val dfJoined = dfMatchesFranceAfter1980.join(dfStatsWritten, "adversaire")

    // Printing df schema and some rows of result and writing into parquet file
    dfJoined.printSchema()
    dfJoined.show(100)
    dfJoined.write.mode("overwrite").parquet("data/result.parquet")

    // Stopping the spark session
    spark.stop()
  }
}
