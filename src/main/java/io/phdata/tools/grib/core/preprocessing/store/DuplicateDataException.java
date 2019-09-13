package io.phdata.tools.grib.core.preprocessing.store;

/**
 * Created by cisaksson on 8/12/19.
 */
public class DuplicateDataException extends GribCreationException {

  private static final long serialVersionUID = 5253976999274387552L;

  public DuplicateDataException(String reason) {
    super(reason);
  }
}