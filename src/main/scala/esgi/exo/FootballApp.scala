package esgi.exo

import org.apache.spark.sql.SparkSession

object FootballApp {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Program")
      .config("spark.master", "local")
      .getOrCreate()

    val df_matches = spark
      .read
      .option("header", value = true)
      .csv("input/df_matches.csv")

    spark.stop()
  }
}
