package questions

import data.FlightData

import scala.io.Source
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date

class FlatFileLoader {

  /** load csv file into dataset or dataframe from csv path.
    *
    * @param spark    entry point of sparkSession
    * @param path     the customized directory that the csv locates
    * @return dataframe
    */
  def loadCsvByPath(spark: SparkSession, path: String): Dataset[Row] = {

    val srcDF: Dataset[Row] = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

    srcDF
  }

  /** save dataset or dataframe into csv file by giving save mode and path.
    *
    * @param resDS the result dataset or dataframe that needs to save
    * @param saveMode  save mode of overwrite, append, ignore, errorIfExists
    * @param path the customized directory that the csv generates
    * @return nothing to return
    */
  def saveCsvByPath(resDS: Dataset[Row], saveMode: SaveMode, path: String) = {
    val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //val filename = fmt.format(new Date())
    resDS
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .mode(saveMode)
      .option("header", true)
      .csv(path)
  }
}
