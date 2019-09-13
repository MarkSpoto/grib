package io.phdata.tools.grib.core.common;

import io.phdata.tools.grib.core.preprocessing.store.GribCreationException;
import java.util.List;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dt.GridCoordSystem;
import ucar.nc2.dt.grid.GridDataset;

/**
 * Handling data for Grib Objects of this class may be used to add time
 * dimensions and -variables to CDM objects, and to get values for the
 * same variables.
 * Created by cisaksson on 8/12/19.
 */
public interface DataHandler {

  /**
   *
   * @param netcdfDataset The main object for NetCDF
   * @return Gets te Grid dataset object
   */
  GridDataset getGridDataset(NetcdfDataset netcdfDataset);

  /**
   *
   * @param gridDataset The data set from the NetCDF object
   * @return The grid coord system
   * @throws GribCreationException exception
   */
  GridCoordSystem getCoordinateSystem(GridDataset gridDataset) throws GribCreationException;

  /**
   * Add relevant dimensions and -variables to the given CDM object.
   */
  void convertToLatLon();

  /**
   * Get data for the given variable name and unlimited dimension
   */
  List<Double> getLatitude();

  /**
   * Does the given cdm variable name refer to anything that this object can
   * handle via the getData method?
   */
  List<Double> getLongitude();

  /**
   *
   * @return the coordinates.
   */
  GridCoordSystem getCoordinates();
}