package io.phdata.tools.grib.spark.utility

import io.phdata.tools.grib.spark.utility.impl.IOServicesImpl
import org.apache.spark.sql.types.{StringType, StructField, StructType}


/**
 * Created by cisaksson on 8/12/19.
 */
object CreateSchema {
  var ELEMENT_DELIMITER = ","

  /**
   * This function dynamically read the Spark DataFrame schema from a file
   * with the following format, <featureName,dataType>:
   *
   * ex:
   * featureName1,string
   * featureName2,float
   * featureName3,string
   * ...
   *
   * @param getAbsolutePath The path to the schema file
   * @return Spark DataFrame schema
   */
  def getSchema(getAbsolutePath: String): (StructType, Array[String]) = {
    val schema = IOServicesImpl.readFile(getAbsolutePath)

    var structField: Array[StructField] = Array()
    var name: Array[String] = Array()
    for (row <- schema.split("\n")) {
      val cName: String = row.split(ELEMENT_DELIMITER).head
      structField :+= StructField(cName,
        AutoDetectDataTypes.nameToType(row.split(ELEMENT_DELIMITER).last), nullable = true)
      name :+= cName
    }
    (new StructType(structField), name)
  }

  /**
   * Get the structType schema from a scala sequence list
   *
   * @param columnsNames Sequence list of feature names
   * @return Spark DataFrame schema
   */
  def getStrTypeSchema(columnsNames: Seq[String]): StructType = {
    StructType(columnsNames.map(fieldName => StructField(fieldName, StringType, nullable = true)))
  }
}
