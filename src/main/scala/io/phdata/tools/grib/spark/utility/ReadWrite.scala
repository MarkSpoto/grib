package io.phdata.tools.grib.spark.utility

import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable}

/**
 * Extended trait to make the default implementation public.
 */
trait BasicParamsWritable extends DefaultParamsWritable {
  self: Params =>
}

/**
 * Extended trait to make the default implementation public.
 */
trait BasicParamsReadable[T] extends DefaultParamsReadable[T] {}
