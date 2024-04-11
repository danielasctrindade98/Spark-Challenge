package app

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class Part2(data: DataFrame) {

  def execute(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val best_apps = data
      // Filter only the non null and non NaN values
      .filter($"Rating".isNotNull)
      .filter($"Rating" =!= "NaN")

      // Obtain all Apps with a "Rating" greater or equal to 4.0 sorted
      .filter($"Rating" >= 4.0)
      .filter($"Rating" >= 4.0)

      // Sort it in descending order
      .sort(desc("Rating"))

    best_apps

  }

}