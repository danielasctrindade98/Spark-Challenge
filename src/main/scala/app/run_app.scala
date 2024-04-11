package app

import org.apache.spark.sql.SparkSession

object run_app {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("AtomicEngineering")
      .getOrCreate()

    // Load the datasets into DataFrames
    val df_GooglePlayStore = spark.read.format("csv").option("header", "true")
      .load("Data/googleplaystore.csv")
    val df_GooglePlayStoreUserReviews = spark.read.format("csv").option("header", "true")
      .load("Data/googleplaystore_user_reviews.csv")

    //Part 1
    val part_1 = Part1(df_GooglePlayStoreUserReviews) //Define Dataframe to use
    val df_1 = part_1.execute(spark)

    //Part 2
    val part_2 = Part2(df_GooglePlayStore)
    val df_2 = part_2.execute(spark)
    //Save CSV file
    df_2.write.mode("overwrite").option("delimiter", "§").option("encoding", "UTF-8").csv("./Output_Files/best_apps.csv")

    //Part 3
    val part3 = Part3(df_GooglePlayStore)
    val df_3 = part3.execute(spark)

    //Part 4
    val part4 = Part4(df_1,df_3)
    val df_part4 = part4.execute(spark)

    //Part 5
    // In this part it's asked to use df_3 to to return a "df_4" dataframe with the number of applications,
    // the average rating and the average sentiment polarity variables. For this it's necessary to join df_1 and df_3.
    // Because this was done in step 4 previously, it's going to be used the df_part4 instead of df_3 dataframe.

    val part5 = Part5(df_part4)
    val df_4 = part5.execute(spark)

    spark.stop()
  }
}