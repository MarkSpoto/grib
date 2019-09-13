package io.phdata.tools.grib.spark.utility

import org.apache.hadoop.fs.Path

/**
 * Created by cisaksson on 8/12/19.
 */
trait IOServices {
  def readFile(absolutePath: String): String
  def readBinFile(absolutePath: String): Array[Byte]
  def readFileAsSeq(absolutePath: String): Seq[String]
  def readFileFromHDFS(location: String): Array[String]
  def writeFileToHDFS(absolutePath: String, inputData: String)
  def merge(srcPath: String, dstPath: String): Unit
  def cleanup(absolutePath: String): Path
  def mkdir(absolutePath: Path): Boolean
  def mkFile(absolutePath: String): Boolean
  def filExists(absolutePath: String): Boolean
}
