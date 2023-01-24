package stock_analysis


import org.apache.spark.sql.SparkSession
import stock_analysis.sparkWrapper.getSpark
import stock_analysis.stock_report.generate_summary_report
import stock_analysis.util.{dataWriter, loadSourceData, symbol_meta_cls}

object report {

  def main(args: Array[String]): Unit = {
    implicit lazy val spark: SparkSession = getSpark
    var accessKeyId: String = sys.env.getOrElse("AWS_ACCESS_KEY_ID", "Undefined")
    val secretAccessKey: String = sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", "Undefined")
    val sc = spark.sparkContext
    //  s3a Hadoop settings
    sc.hadoopConfiguration.set("fs.s3a.access.key", accessKeyId)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", secretAccessKey)
    sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    spark.sparkContext.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    import spark.implicits._

    val readOpts = Map("sep" -> ",", "format" -> "csv")
    val writeOpts = Map("sep" -> ",", "format" -> "csv", "mode" -> "overwrite")
    val basePath = "/Users/Pritam.Bhandare/stock_analysis-main"
    val symbol_meta_data = loadSourceData(spark, basePath + "/symbol_metadata.csv", readOpts).cache()
    val symbol_meta = symbol_meta_data.as[symbol_meta_cls].collectAsList()

    val all_Time_Summary_df = generate_summary_report(spark, basePath, readOpts)
    val range_Time_Summary_df = generate_summary_report(spark, basePath, readOpts, "2021-01-12", "2022-03-14")
    val range_Time_detail_Summary_df = generate_summary_report(spark, basePath, readOpts, "2021-02-12", "2022-03-14", "detail")

    dataWriter(all_Time_Summary_df, basePath + "/all_time_summary", writeOpts)
    dataWriter(range_Time_Summary_df, basePath + "/range_time_summary", writeOpts)
    dataWriter(range_Time_detail_Summary_df, basePath + "/range_time_detail_summary", writeOpts)
  }

}
