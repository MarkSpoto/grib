package io.phdata.tools.grib.core.preprocessing.store;

/**
 * Created by cisaksson on 8/12/19.
 */
public class GribCreationException extends Exception {

  private static final long serialVersionUID = 7925913249108290301L;

  public GribCreationException(String reason) {
    super(reason);
  }
}