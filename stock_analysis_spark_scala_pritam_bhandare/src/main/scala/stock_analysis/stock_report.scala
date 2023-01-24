package stock_analysis

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col, current_date, lit, max, min, row_number, to_date}
import org.apache.spark.sql.types.{DataTypes, IntegerType}
import org.apache.spark.storage.StorageLevel
import stock_analysis.report.symbol_meta
import stock_analysis.util.{isValidDate, loadSourceData}

object stock_report extends App {

  /**
   *
   * @param data : Dataframe loaded from [generate_summary_report]
   * @param flag : Used to fetch columns from dataframe based
   * @return
   */
  def agg_result(data: DataFrame, flag: String = ""): DataFrame = {
    val agg_cols = List("Sector", "Avg_Open_Price", "Avg_Close_Price", "Max_High_Price", "Min_Low_Price", "Avg_Volume")
    val detail_agg_cols = List("Sector", "Name", "Avg_Open_Price", "Avg_Close_Price", "Max_High_Price", "Min_Low_Price", "Avg_Volume")
    val stockType = DataTypes.createDecimalType(16, 2)
    val spec = Window.partitionBy(col("Sector")).orderBy(col("timestamp").desc)
    val res_df = data.withColumn("Avg_Open_Price", avg(col("open")).over(spec).cast(stockType))
      .withColumn("Avg_Close_Price", avg(col("close")).over(spec).cast(stockType))
      .withColumn("Max_High_Price", max(col("high")).over(spec).cast(stockType))
      .withColumn("Min_Low_Price", min(col("low")).over(spec).cast(stockType))
      .withColumn("Avg_Volume", avg(col("volume")).over(spec).cast(IntegerType))
      .withColumn("rnk", row_number().over(spec))
      .filter("rnk=1")

    flag.toLowerCase match {
      case "detail" => res_df.select(detail_agg_cols.head, detail_agg_cols.tail: _*)
      case _ => res_df.select(agg_cols.head, agg_cols.tail: _*)
    }
  }

  /**
   *
   * @param spark     :Sparksession
   * @param basePath  :basepath where all files located
   * @param readOpts  : Map of config like delimiter, fileformat, etc
   * @param startDate : to filter records from date
   * @param endDate   : to filter records till date
   * @param flag      : Used to fetch columns from dataframe based
   * @return
   */
  def generate_summary_report(spark: SparkSession, basePath: String, readOpts: Map[String, String], startDate: String = "", endDate: String = "", flag: String = ""): DataFrame = {
    val readFormat = readOpts.getOrElse("format", "")
    if (startDate.nonEmpty) {
      if (!isValidDate(startDate)) throw new Exception("Provide valid startDate")
    }
    if (endDate.nonEmpty) {
      if (!isValidDate(endDate)) throw new Exception("Provide valid endDate")
    }

    val stock_data_schema = "timestamp Date, open Decimal, high Decimal, low Decimal, close Decimal, volume Integer"

    val dfs_lis = for (s <- 0 until symbol_meta.size()) yield {
      val meta = symbol_meta.get(s)
      val path = basePath.replaceAll("/$", "").trim + "/" + meta.symbol + "." + readFormat.toLowerCase

      val data = loadSourceData(spark, path, readOpts, stock_data_schema)
        .withColumn("Symbol", lit(meta.symbol))
        .withColumn("Name", lit(meta.name))
        .withColumn("Sector", lit(meta.sector))
        .withColumn("timestamp", to_date(col("timestamp")))
        .filter(col("timestamp") < current_date())

      val filterEndDT = if (endDate.nonEmpty) to_date(lit(endDate)) else current_date()
      if (startDate.nonEmpty) {
        data.filter(col("timestamp") > to_date(lit(startDate)) && col("timestamp") < filterEndDT)
      } else data.filter(col("timestamp") < filterEndDT)
    }
    val filterd_df = dfs_lis.reduce(_ unionByName _)
    filterd_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    agg_result(filterd_df, flag)
  }


}
