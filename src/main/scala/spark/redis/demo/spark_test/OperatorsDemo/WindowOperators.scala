package spark.redis.demo.spark_test.OperatorsDemo

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class WindowOperators(spark:SparkSession) {

  import spark.implicits._

  /**
   * window operators
   * @param df
   * @return
   */
  def windowGroupCount(df:DataFrame): DataFrame ={
    val windowcount = df.groupBy(
      window($"timestamp", "3 minutes", "1 minutes"),
      $"value"
    ).count()
    windowcount
  }

  /**
   * output mode must be Append or Update
   * @param df DataFrame
   * @return
   */
  def winddowWatermarking(df:DataFrame): DataFrame={
    val windowcount = df
      .withWatermark("timestamp", "6 minutes")
      .groupBy(
        window($"timestamp", "6 minutes", "3 minutes"),
        $"value"
    ).count()
    windowcount
  }
}
