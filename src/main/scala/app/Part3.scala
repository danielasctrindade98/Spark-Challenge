package app

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

case class Part3(data: DataFrame) {
  private var result: DataFrame = _

  def execute(spark: SparkSession): DataFrame = {
    import spark.implicits._

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    val cleanedDF = data.select(
      $"App",
      $"Category".alias("Categories"),
      when(isnan($"Rating"), lit(null)).otherwise($"Rating".cast("Double")).alias("Rating"),
      $"Reviews".cast("Long").alias("Reviews"),
      when($"Size".endsWith("k"),
        (regexp_extract($"Size", "(\\d+\\.?\\d*)", 1).cast("Double") / 1000).alias("Size")
      ).when($"Size".endsWith("M"),
        regexp_extract($"Size", "(\\d+\\.?\\d*)", 1).cast("Double").alias("Size")
      ).otherwise(lit(null)).alias("Size"),
      $"Installs",
      $"Type",
      (regexp_replace($"Price", "[$]", "").cast("Double") * 0.9).alias("Price"),
      $"Content Rating".alias("Content_Rating"),
      split($"Genres", ";").alias("Genres"),
      to_date(date_format(to_date($"Last Updated", "MMMM dd, yyyy"), "yyyy-MM-dd")).alias("Last_Updated"),
      $"Current Ver".alias("Current_Version"),
      $"Android Ver".alias("Minimum_Android_Version")
    )

    val categoriesByApp = cleanedDF
      .groupBy("App")
      .agg(
        sort_array(collect_set($"Categories")).alias("Categories"),
        first($"Rating").alias("Rating"),
        first($"Reviews").alias("Reviews"),
        first($"Size").alias("Size"),
        first($"Installs").alias("Installs"),
        first($"Type").alias("Type"),
        first($"Price").alias("Price"),
        first($"Content_Rating").alias("Content_Rating"),
        first($"Genres").alias("Genres"),
        first($"Last_Updated").alias("Last_Updated"),
        first($"Current_Version").alias("Current_Version"),
        first($"Minimum_Android_Version").alias("Minimum_Android_Version"))
      .orderBy("App")

    val windowSpec = Window.partitionBy("App").orderBy($"Reviews".desc)

    val maxReviewsNumberDF = categoriesByApp
      .withColumn("max_reviews", max("Reviews").over(windowSpec))
      .filter($"Reviews" === $"max_reviews")
      .drop("max_reviews")

    result = maxReviewsNumberDF
    result
  }

  def getResult: DataFrame = result
}