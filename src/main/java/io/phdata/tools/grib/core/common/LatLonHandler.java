package io.phdata.tools.grib.core.common;

import io.phdata.tools.grib.core.preprocessing.store.GribCreationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import ucar.ma2.Array;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dt.GridCoordSystem;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;


/**
 * Created by cisaksson on 8/14/19.
 */
public class LatLonHandler implements DataHandler {
    private static final Log LOG = LogFactory.getLog(LatLonHandler.class);

    private List<Double> latitude;
    private List<Double> longitude;
    private GridCoordSystem coordinates;
    private ProjectionSpecification projectionSpec;

    public LatLonHandler(NetcdfDataset netcdfDataset)
        throws IOException, GribCreationException {
        GridDataset gridDataset = getGridDataset(netcdfDataset);
        this.coordinates = getCoordinateSystem(gridDataset);
        this.projectionSpec = new ProjectionSpecification(netcdfDataset, GridMapping.LATLON_PROJECTION);
        convertToLatLon();
    }

    @Override
    public GridDataset getGridDataset(NetcdfDataset netcdfDataset) {
        GridDataset gridDataset = null;
        try {
            gridDataset =  new GridDataset(netcdfDataset);
        } catch (IOException ex) {
            LOG.error("GribBuilder::getGridDataset problem getting the grid dataset", ex);
        }
        return gridDataset;
    }

    @Override
    public GridCoordSystem getCoordinateSystem(GridDataset gridDataset) throws GribCreationException {
        if(gridDataset != null) {
            GridDatatype datatype = gridDataset.getGrids().iterator().next();
            return datatype.getCoordinateSystem();
        }
        throw new GribCreationException("Grid dataset can't be null");
    }

    @Override
    public void convertToLatLon() {
        this.latitude = new ArrayList<>();
        this.longitude = new ArrayList<>();
        Array lat = projectionSpec.getxData();
        Array lon = projectionSpec.getyData();

        for (int i = 0; i < lat.getSize(); i++) {
          for (int j = 0; j < lon.getSize(); j++) {
            latitude.add(lat.getDouble(i));
            longitude.add(lon.getDouble(j));
          }
        }
    }

    @Override
    public List<Double> getLatitude() {
        return latitude;
    }

    @Override
    public List<Double> getLongitude() {
        return longitude;
    }

    @Override
    public GridCoordSystem getCoordinates() {
        return coordinates;
    }
}