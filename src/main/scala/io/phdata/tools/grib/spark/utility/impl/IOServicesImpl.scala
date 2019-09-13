package io.phdata.tools.grib.spark.utility.impl

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{Path, _}

import scala.io.{BufferedSource, Source}
import org.apache.hadoop.conf.Configuration
import java.io.{BufferedWriter, InputStream, OutputStreamWriter, StringWriter}

import io.phdata.tools.grib.spark.utility.IOServices
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory}
import org.apache.hadoop.mapred.FileInputFormat

import scala.reflect.io.Streamable.Bytes


/**
 * Created by cisaksson on 8/12/19.
 */
object IOServicesImpl extends IOServices{
  val cof = new Configuration()
  val fs: FileSystem = FileSystem.get(this.cof)

  /**
   * This function gets all file from a path
   * @param path to the directory where all the grib files are stored on HDFS
   * @return Seq of file fonds in directory
   */
  def getAllFiles(path:String): Seq[String] = {
    val files = this.fs.listStatus(new Path(path))
    files.map(_.getPath.toString)
  }

  /**
   * This function reads a text file and returns the content as a string from
   * the local disk.
   *
   * @param absolutePath The file path
   * @return A file content as a string
   */
  override def readFile(absolutePath: String): String = {
    val stream: InputStream = getClass.getResourceAsStream(absolutePath)

    if (stream == null)
      throw new IllegalArgumentException(s"File <'$absolutePath'> does not exist.")

    IOUtils.toString(stream, "UTF-8")
  }

  /**
   * This function reads a bin file and returns the content as a string from
   * the local disk.
   *
   * @param absolutePath The file path
   * @return A file content as a string
   */
  override def readBinFile(absolutePath: String): Array[Byte] = {
    val binStream = fs.open(new Path(absolutePath))

    if (binStream == null)
      throw new IllegalArgumentException(s"File <'$absolutePath'> does not exist.")

    IOUtils.toByteArray(binStream)
  }

  /**
   * This function reads a text file and returns a sequence list of String
   * types from the local disk.
   *
   * @param absolutePath The file path
   * @return Sequence list of String types
   */
  override def readFileAsSeq(absolutePath: String): Seq[String] = {
    val stream: InputStream = getClass.getResourceAsStream(absolutePath)
    val bufferedSource: BufferedSource = Source.fromInputStream(stream)
    var lines: Seq[String] = null
    try {
      lines = (for (line <- bufferedSource.getLines()) yield line).toList
    } finally {
      bufferedSource.close()
    }
    lines
  }

  /**
   * This function reads a text file from HDFS and returns a list of String.
   * each list item represents a one line in the text file.
   *
   * @param location The file path
   * @return Content of the text file
   */
  override def readFileFromHDFS(location: String): Array[String] = {
    try {
      val factory: CompressionCodecFactory = new CompressionCodecFactory(this.cof)
      val items: Array[FileStatus] = this.fs.listStatus(new Path(location))

      var results: Array[String] = Array.empty[String]
      for (item <- items) {
        // ignoring files like _SUCCESS
        if (!item.getPath.getName.startsWith("_")) {

          val codec: CompressionCodec = factory.getCodec(item.getPath)
          var stream: InputStream = null

          // check if we have a compression codec we need to use
          if (codec != null) {
            stream = codec.createInputStream(this.fs.open(item.getPath))
          } else {
            stream = this.fs.open(item.getPath)
          }

          val writer: StringWriter = new StringWriter()
          IOUtils.copy(stream, writer, "UTF-8")
          val raw: String = writer.toString
          for (str <- raw.split("\n")) {
            results :+= str
          }
        }
      }
      results
    } catch {
      case ex: Exception => Array.empty[String]
    }
  }

  /**
   * This function writes a string content to a text file into HDFS.
   *
   * @param absolutePath The file path
   * @param inputData The string to be writen
   */
  override def writeFileToHDFS(absolutePath: String, inputData: String): Unit = {
    val path: Path = cleanup(absolutePath)

    val dataOutputStream: FSDataOutputStream = fs.create(path)
    val bw: BufferedWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream, "UTF-8"))
    try {
      bw.write(inputData)
    } finally {
      bw.close()
    }
  }

  /**
   * This helper function is used to merge all the parts created in HDFS to a single file.
   *
   * @param srcPath The path to all the parts
   * @param dstPath The putput directory
   */
  override def merge(srcPath: String, dstPath: String): Unit = {
    FileUtil.copyMerge(fs, new Path(srcPath), fs, new Path(dstPath), true, this.cof, null)
  }

  /**
   * This helper function remove all files and directories if exists in HDFS.
   *
   * @param absolutePath The root path
   * @return The path object
   */
  override def cleanup(absolutePath: String): Path = {
    val path: Path = new Path(absolutePath)

    if (this.fs.exists(path)) {
      this.fs.delete(path, true)
    }
    path
  }

  /**
   * Creates a directory in HDFS if not exists.
   *
   * @param absolutePath The root path
   * @return true if successful else false
   */
  override def mkdir(absolutePath: Path): Boolean = {
    this.fs.mkdirs(absolutePath)
  }

  /**
   * Helper function to create a empty file in HDFS.
   * @param absolutePath The root path
   * @return true if successful else false
   */
  override def mkFile(absolutePath: String): Boolean = {
    this.fs.createNewFile(new Path(absolutePath))
  }

  /**
   * Helper function to test if file exists.
   *
   * @param absolutePath The root path
   * @return true if successful else false
   */
  override def filExists(absolutePath: String): Boolean = {
    this.fs.exists(new Path(absolutePath))
  }
}
