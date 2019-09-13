package io.phdata.tools.grib.core.common;

/**
 * Created by cisaksson on 8/14/19.
 */
public enum GridMapping {
  LATLON_PROJECTION("LatLon_Projection", 0),
  LAMBERT_CONFORMAL_PROJECTION("LambertConformal_Projection", 1);

  private final String name;
  private final int num;

  private GridMapping(String name, int num) {
    this.name = name;
    this.num = num;
  }

  public String getName() {
    return this.name;
  }

  public int getNum() {
    return num;
  }
}
