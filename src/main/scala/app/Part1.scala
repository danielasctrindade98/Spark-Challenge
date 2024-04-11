package app

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class Part1(data: DataFrame) {
  // Create private variable to store the result
  private var data_result: DataFrame = _

  def execute(spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Average sentiment polarity grouped by the column “App” from the input data DataFrame.

    data_result = data.groupBy("App").agg(avg($"Sentiment_Polarity").alias("Average_Sentiment_Polarity"))
      .na.fill(0, Seq("Average_Sentiment_Polarity"))

    // The result is stored in the result DataFrame.
    data_result
  }
}