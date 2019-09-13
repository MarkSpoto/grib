package io.phdata.tools.grib.spark.modules

import java.io.{File, FileOutputStream}
import java.nio.file.Files
import java.util

import io.phdata.tools.grib.core.preprocessing.GribFileBuilder
import io.phdata.tools.grib.core.preprocessing.store.{GribFileContainer, GribRow}
import io.phdata.tools.grib.core.util.FileUtil
import io.phdata.tools.grib.spark.frame.{FrameConf, FrameContext}
import io.phdata.tools.grib.spark.utility.{BasicParamsWritable, MetricArrayParam}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SaveMode}
import org.slf4j.{Logger, LoggerFactory}
import resource._
import scalaj.http.{Http, HttpOptions}
import ucar.nc2.dt.grid.GridDataset

import scala.util.hashing.MurmurHash3


/**
 * Created by cisaksson on 8/12/19.
 */
class TransLevelDataPullImpl(override val uid: String) extends Transformer with BasicParamsWritable {
  private val logger: Logger = LoggerFactory.getLogger(classOf[TransLevelDataPullImpl])

  private val conf: FrameConf = new FrameConf()
  def this() = this(Identifiable.randomUID("TransLevelDataPullImpl"))

  /**
   * Param for the column name that holds the GRIB download url.
   * @group param
   */
  final val gribUrlCol: Param[String] =
    new Param[String](this, "gribUrlCol", "column name for the GRIB download url")

  /** @group setParam */
  def setGribUrlColumn(column: String): this.type = set(gribUrlCol, column)

  /** @group getParam */
  def getGribUrlColumn: String = $(gribUrlCol)

  /**
   * Param for the folder path where downloaded GRIB files will be saved.
   * @group param
   */
  final val gribsPath: Param[String] =
    new Param[String](this, "gribsPath", "folder path where downloaded GRIB files will be saved")

  /** @group setParam */
  def setGribsPath(path: String): this.type = set(gribsPath, path)

  /** @group getParam */
  def getGribsPath: String = $(gribsPath)

  /**
   * Param for the weather metrics to retrieve.
   * @group param
   */
  final val metrics: MetricArrayParam =
    new MetricArrayParam(this, "metrics", "weather metrics to retrieve")

  /** @group setParam */
  def setMetrics(metricsSeq: Array[Metric]): this.type = set(metrics, metricsSeq)

  /** @group getParam */
  def getMetrics: Array[Metric] = $(metrics)

  /**
   * Param for the column name that holds the latitude coordinate.
   * @group param
   */
  final val latitudeCol: Param[String] =
    new Param[String](this, "latitudeCol", "column name for the latitude coordinate")

  /** @group setParam */
  def setLatitudeColumn(column: String): this.type = set(latitudeCol, column)

  /** @group getParam */
  def getLatitudeColumn: String = $(latitudeCol)

  /**
   * Param for the column name that holds the longitude coordinate.
   * @group param
   */
  final val longitudeCol: Param[String] =
    new Param[String](this, "longitudeCol", "column name for the longitude coordinate")

  /** @group setParam */
  def setLongitudeColumn(column: String): this.type = set(longitudeCol, column)

  /** @group getParam */
  def getLongitudeColumn: String = $(longitudeCol)


  setDefault(
    gribUrlCol -> "grib_url",
    gribsPath -> new File(sys.props.get("java.io.tmpdir").get, "grib").getAbsolutePath,
    metrics -> Array(Metric.Pressure, Metric.Temperature, Metric.Humidity),
    latitudeCol -> "lat",
    longitudeCol -> "lon"
  )


  /**
   * Normalize the string to avoid any false positive due to whitespace
   * @param s The input string
   * @return A normalized string
   */
  def normalized(s: String): String = s.replaceAll("(?s)\\s+", " ").trim

  /**
   * This helper function extracts the schema from the Spark DataFrame and
   * make sure that data types are compatible with hive
   *
   * @param dataFrame The Spark DataFrame
   * @return The schema
   */
  def getSchema(dataFrame: DataFrame): String = {
    val schema: StructType = dataFrame.schema
    val name = schema.map(f => s"${f.name} ${f.dataType.simpleString}")
    val st_schema: String = name mkString ","

    val schemaFilter: String = st_schema
      .replaceAll("decimal\\([0-9]+,[0-9]+\\)", "double")
      .replaceAll("integer", "int")
      .replaceAll("long", "bigint")

    schemaFilter
  }

  /**
   * Helper function to compare two schemas. It will normalize the strings to
   * avoid any false positive due to whitespace
   *
   * @param schema_a One
   * @param schema_b Two
   * @return if true then matches else not
   */
  def schemaMatching(schema_a: String, schema_b: String): Boolean = {
    val schema_a_norm = normalized(schema_a)
    val schema_b_norm = normalized(schema_b)

    schema_a_norm.equalsIgnoreCase(schema_b_norm)
  }

  /**
   * Is used to test if Hive table exists.
   *
   * @param tableName The Hive table name
   * @param sqlContext The spark context
   * @return If true the table exists else not
   */
  def tableExists(tableName: String, sqlContext: SQLContext): Boolean = {
    sqlContext.tableNames.contains(tableName)
  }

  /**
   * This function write the Spark DataFrame to Hive table.
   *
   * @param tableName The Hive database name and table name: <databaseName.tableName>
   * @param schema The schema used to create Hive table
   * @param mode Overwrite or Append
   * @param dataFrame The spark DataFormat
   * @param sqlContext The spark context
   */
  def createAndInsertHiveTable(tableName: String, schema: String, mode: SaveMode,
                               dataFrame: DataFrame, sqlContext: SQLContext): Unit = {
    val sql_cmd: String = s"CREATE TABLE IF NOT EXISTS $tableName ($schema)"
    logger.debug("SQL: {}", sql_cmd)

    sqlContext.sql(sql_cmd)
    dataFrame.write.mode(mode).format("parquet").insertInto(tableName)
  }

  /**
   * This function write the Spark DataFrame to either CSV or parquet format.
   *
   * @param fileName The path and file name of either a parquet or CSV
   * @param mode Overwrite or Append
   * @param dataFrame The spark DataFormat
   * @param isCsv if true csv format else parquet
   */
  def createAndInsertFile(fileName: String, mode: SaveMode, dataFrame: DataFrame, isCsv: Boolean): Unit = {
    var format: String = "parquet"
    if (isCsv)
      format = "com.databricks.spark.csv"

    dataFrame.write.mode(mode).format(format).save(fileName)
  }

  /**
   * Download the grib from the given url and return it as a [[GridDataset]].
   */
  private def downloadGrib(gribUrl: String, gribsDir: String, metrics: Seq[Metric]): String = {
    val fileName = "gfs_" + MurmurHash3.stringHash(gribUrl).toString + ".grb2"
    val gribFile = new File(gribsDir, fileName)

    // Download file
    if (Files.notExists(gribFile.toPath)) {
      logger.debug(s"=========>> Downloading ${gribFile.getPath}")
      logger.debug(s"=========>> Downloading ${gribUrl}")
      val res = Http(gribUrl).option(HttpOptions.allowUnsafeSSL).asBytes
      if (res.isSuccess) {
        gribFile.getParentFile.mkdirs()
        gribFile.createNewFile()

        for (out <- managed(new FileOutputStream(gribFile, false))) {
          out.write(res.body)
        }
      } else {
        logger.error("Couldn't download grib", new Throwable(s"Got response code ${res.code} for $gribUrl"))
      }
    } else {
      logger.debug(s"Grib ${gribFile.getPath} already downloaded")
    }
    gribFile.getAbsolutePath
  }

  /**
   * The below method reads the provided arguments in the following order and run the spark job to
   * extract data in a distributed fashion.
   *
   * @param args based from the cli module
   */
  def executor(args: Array[String]): Unit = {


    val hdfsFilePath = args(0)

    // hdfsFilePath is the path configured in the workflow.xml. This directory can contain GRIB
    // files or a text file with http path for GRIB files. Below is a path for one GRIB file.
    // This can be automated to look for the newest files and downloaded.
    val uri: String = "https://nomads.ncdc.noaa.gov/data/ndfd/201605/20160502/ZZUZ97_KWBN_201605022333"

    val gribFileName: String = FileUtil.extractFileName(String)
    logger.debug("Grib file: {}", gribFileName)

    // Need to save downloaded GRIB file to a tmp directory.
    val tmpGribsPath: String = new File(sys.props.get("java.io.tmpdir").get, "grib").getAbsolutePath
    val metricsArray = $(metrics)

    val data: String = downloadGrib(uri, tmpGribsPath, metricsArray)
    val fc: FrameContext = new FrameContext(conf)

    val builder: GribFileBuilder = new GribFileBuilder(data, gribFileName)
    val container: GribFileContainer = builder.getContainer

    val dataRow = container.getRows

    // This should work in Spark > 2. If not, Then StructType Schema needs to be used.
    // This should convert the list of GribRow objects to a Spark DataFrame.
    val dataDF = fc.ss.createDataFrame(dataRow, classOf[util.List[GribRow]])
    val jdbcSchema = getSchema(dataDF)
    dataDF.show(100)

    createAndInsertHiveTable(gribFileName, jdbcSchema, SaveMode.Append, dataDF, fc.sqlContext)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    import dataset.sqlContext
    import dataset.sqlContext.{sparkContext => sc}

    val outputSchema = transformSchema(dataset.schema, logging = true)
    val emptyDataset = sqlContext.createDataFrame(sc.emptyRDD[Row], dataset.schema)

    // Column names
    val latCol = $(latitudeCol)
    val lonCol = $(longitudeCol)
    val urlCol = $(gribUrlCol)
    val gribsDir = $(gribsPath)
    val metricsArray = $(metrics)

    emptyDataset

    // Extract all the grib download urls
//    val gribUrls = dataset
//      .select(urlCol)
//      .map(_.getString(0))
//      .distinct()
//      .collect()

    // Group rows with the same url in the same partition
//    val groupedDataset = gribUrls.mapCompose(emptyDataset)(url => groups => {
//      val sameUrlGroup = dataset.filter(col(urlCol) === url).coalesce(1)
//      groups.unionAll(sameUrlGroup)
//    })
//
//    // Process each partition
//    val rows = groupedDataset
//      .mapPartitions { partition =>
//        if (!partition.hasNext) {
//          // Skip empty partitions
//          partition
//        } else {
//          val rows = partition.toList
//          val headRow = rows.head
//
//          // Extract indexes and the download url
//          val latIndex = headRow.fieldIndex(latCol)
//          val lonIndex = headRow.fieldIndex(lonCol)
//          val urlIndex = headRow.fieldIndex(urlCol)
//          val gribUrl = headRow.getString(urlIndex)
//
//          // Download and parse the grib file
//          val data = WeatherProvider.downloadGrib(gribUrl, gribsDir, metricsArray)
//          val datatypes = metricsArray.map { metric =>
//            val datatype = data.findGridDatatype(metric.gridName)
//            if (datatype == null) {
//              logWarning(s"Null datatype found for $metric from $gribUrl")
//            }
//            metric -> datatype
//          }.toMap
//
//          // Process the partition
//          val processedRows = rows.map { row =>
//            val lat = row.getDouble(latIndex)
//            val lon = row.getDouble(lonIndex)
//
//            val metricValues = datatypes.map {
//              case (metric, datatype) =>
//                val Array(x, y) = datatype.getCoordinateSystem.findXYindexFromLatLon(lat, lon, null)
//                datatype.readDataSlice(0, 0, y, x).getDouble(0)
//            }.toArray
//
//            Row.merge(row, Row(metricValues: _*))
//          }
//
//          data.close()
//          processedRows.toIterator
//        }
//      }
//
//    sqlContext.createDataFrame(rows, outputSchema)
  }

  override def copy(extra: ParamMap): TransLevelDataPullImpl = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(gribUrlCol)).dataType
    require(inputType == StringType, s"grib url type must be string type but got $inputType.")

    val columns = $(metrics).map(_.name)
    val columnsString = columns.mkString(", ")

    if (schema.fieldNames.intersect(columns).nonEmpty) {
      throw new IllegalArgumentException(s"Output columns $columnsString already exist.")
    }

    val outputFields = $(metrics).map(m => StructField(m.name, DoubleType, nullable = true))
    StructType(schema.fields ++ outputFields)
  }
}