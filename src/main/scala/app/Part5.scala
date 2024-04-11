package app

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class Part5(df: DataFrame) {
  private var df_4: DataFrame = _

  def execute(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val resultDF = df.withColumn("Genre", explode($"Genres"))

    df_4 = resultDF
      .groupBy("Genre")
      .agg(
        countDistinct("App").alias("Count"),
        avg("Rating").alias("Average_Rating"),
        avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
      )

    // Save df4 as a Parquet file with gzip compression
    df_4.write.mode("overwrite").option("compression","gzip").parquet("./Output_Files/googleplaystore_metrics.parquet")

    df_4
  }
}