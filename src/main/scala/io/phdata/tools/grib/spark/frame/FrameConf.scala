package io.phdata.tools.grib.spark.frame

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import org.apache.spark.SparkConf


/**
 * Created by cisaksson on 8/12/19.
 */
class FrameConf extends Cloneable {
  val sparkConf = new SparkConf()
  var resourceFormatMap: Map[String, Any] = _
  private val settings = new ConcurrentHashMap[String, String]()

  def set(key: String, value: String): FrameConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    settings.put(key, value)
    this
  }

  def setResourceFormatMap(formatMap: Map[String, Any]): Unit = resourceFormatMap = formatMap

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key))
  }

  /** Get a parameter as an integer, falling back to a default if not set */
  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /** Get a parameter as a double, falling back to a default if not set */
  def getDouble(key: String, defaultValue: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** Copy this object */
  override def clone: FrameConf = {
    val cloned = new FrameConf()
    settings.entrySet().asScala.foreach { e =>
      cloned.set(e.getKey, e.getValue)
    }
    cloned.setResourceFormatMap(resourceFormatMap)
    cloned
  }
}