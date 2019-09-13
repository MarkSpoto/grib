package io.phdata.tools.grib.spark.frame

import org.apache.spark.sql


/**
 * Created by cisaksson on 8/12/19.
 */
class FrameContext(conf: FrameConf) extends Serializable {
  val ss = new sql.SparkSession.Builder().config(conf.sparkConf).enableHiveSupport().getOrCreate()
  val sc = ss.sparkContext
  val sqlContext = ss.sqlContext

  // If alternate user name was not provided, set user to
  // the current user.
  conf.set("frame.user.name", conf.get("frame.user.name", System.getProperty("user.name")))
  def getConf: FrameConf = conf.clone()
  def isEmpty(x: String): Boolean = Option(x).forall(_.isEmpty)
}