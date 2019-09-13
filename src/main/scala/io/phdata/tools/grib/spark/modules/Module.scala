package io.phdata.tools.grib.spark.modules

/**
 * Created by cisaksson on 8/12/19.
 */
trait Module[A] {
  def executor(args: A)
}
