package stock_analysis

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.time.{LocalDate}
import java.time.format.{DateTimeFormatter, ResolverStyle}
import scala.util.{Failure, Success, Try}

object util {

  //Defining case class for symbol_metadata file
  case class symbol_meta_cls(symbol: String, name: String, address: String, country: String, sector: String, industry: String)

  /**
   *
   * @param spark     : SparkSession
   * @param path      : file source path
   * @param opt       : Map of config like delimiter, fileformat, etc
   * @param schemaStr : Optional - to handle bad records for as per schema provide in string (NOTE- Used when reading symbolwise csv files)
   * @return
   */
  def loadSourceData(spark: SparkSession, path: String, opt: Map[String, String], schemaStr: String = ""): DataFrame = {
    val dataFormat = opt.getOrElse("format", "")
    val source_data = dataFormat.trim.toLowerCase match {
      case "csv" =>
        val seps = Set("delimiter", "sep").map(_.toLowerCase)
        var delim = opt.find(e => seps.contains(e._1.trim.toLowerCase)).map(_._2.trim).getOrElse("")
        if (delim.isEmpty) delim = "," // default delimiter if not provide

        val reader = spark.read
          .format("csv")
          .option("sep", delim)
          .option("inferSchema", false)
          .option("header", true)

        if (schemaStr.nonEmpty) {
          val bad_rec_path = path + "_bad_recs"
          reader.schema(schemaStr).option("dateformat", "yyyy-MM-dd")
            .option("mode", "DROPMALFORMED")
            .option("badRecordsPath", bad_rec_path)
            .load(path)
        } else reader.load(path)

      case _ =>
        spark.read.parquet(path)
    }

    source_data
  }

  /**
   *
   * @param data    : dataframe for write
   * @param dest    : destination path
   * @param opt     : Map of config like delimiter, fileformat, mode, etc
   * @param numpart : Optional - To reduce number of part file to be generated
   */
  def dataWriter(data: DataFrame, dest: String, opt: Map[String, String], numpart: Int = 8): Unit = {
    val dataFormat = opt.getOrElse("format", "")
    val save_mode = ofSaveMode(opt.getOrElse("mode", "append"))

    dataFormat.trim.toLowerCase match {
      case "csv" =>
        val seps = Set("delimiter", "sep").map(_.toLowerCase)
        var delim = opt.find(e => seps.contains(e._1.trim.toLowerCase)).map(_._2.trim).getOrElse("")
        if (delim.isEmpty) delim = ","

        data
          .coalesce(numpart)
          .write
          .format("csv")
          .option("sep", delim)
          .option("header", true)
          .option("ignoreLeadingWhiteSpace", false)
          .option("ignoreTrailingWhiteSpace", false)
          .option("emptyValue", null)
          .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS")
          .mode(save_mode)
          .save(dest)

      case _ =>
        data
          .coalesce(numpart)
          .write
          .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS")
          .mode(save_mode)
          .parquet(dest)
    }
  }

  def ofSaveMode(mode: String): SaveMode = mode.trim.toLowerCase match {
    case "append" => SaveMode.Append
    case "overwrite" => SaveMode.Overwrite
    case _ => SaveMode.ErrorIfExists
  }

  // Date validator
  def isValidDate(dt: String): Boolean = {
    Try(LocalDate.parse(dt, DateTimeFormatter.ofPattern("uuuu-MM-dd").withResolverStyle(ResolverStyle.STRICT))) match {
      case Success(dd) => true
      case Failure(ex) => false
    }
  }

}
