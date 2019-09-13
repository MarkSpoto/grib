package io.phdata.tools.grib

import io.phdata.tools.grib.spark.modules.TransLevelDataPullImpl

/**
 * Created by cisaksson on 8/12/19.
 */
object InvocationServices {
  def main(args: Array[String]): Unit = {
    val Module = new TransLevelDataPullImpl()
    Module.executor(args)
  }
}
