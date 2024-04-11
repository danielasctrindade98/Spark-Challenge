package app

import org.apache.spark.sql.{DataFrame, SparkSession}

case class Part4(df1: DataFrame, df3: DataFrame) {
  private var appJoin_df: DataFrame = _
  def execute(spark: SparkSession): DataFrame = {

    // Left join on 'App'
    val appJoin_df = df3.join(df1.select("App", "Average_Sentiment_Polarity"), Seq("App"), "left")

    // Join Everything
    val df_part4 = appJoin_df.select(df3.col("*"), df1.col("Average_Sentiment_Polarity")).orderBy("App")

    // Save df_part4 as a Parquet file with gzip compression
    df_part4.write.mode("overwrite").option("compression","gzip").parquet("./Output_Files/googleplaystore_cleaned.parquet")

    df_part4
  }
}

