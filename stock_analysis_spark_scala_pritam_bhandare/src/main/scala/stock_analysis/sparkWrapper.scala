package stock_analysis

import org.apache.spark.sql.SparkSession

object sparkWrapper {

  def getSpark: SparkSession = {
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    SparkSession.builder()
      //      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      //      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.driver.maxResultSize", "5g")
      .master("local[*]")
      .appName("practice_test")
      .getOrCreate()
  }


}
