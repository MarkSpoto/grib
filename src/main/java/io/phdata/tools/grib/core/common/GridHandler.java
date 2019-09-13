package io.phdata.tools.grib.core.common;

import io.phdata.tools.grib.core.preprocessing.store.GribCreationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dt.GridCoordSystem;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;
import ucar.unidata.geoloc.ProjectionImpl;

/**
 * Created by cisaksson on 8/14/19.
 */
public class GridHandler implements DataHandler {
    private static final Log LOG = LogFactory.getLog(GridHandler.class);

    private List<Double> latitude;
    private List<Double> longitude;
    private GridCoordSystem coordinates;
    private ProjectionImpl projection;
    private ProjectionSpecification projectionSpec;

    public GridHandler(NetcdfDataset netcdfDataset)
        throws GribCreationException, IOException {
        GridDataset gridDataset = getGridDataset(netcdfDataset);
        this.coordinates = getCoordinateSystem(gridDataset);
        this.projection = coordinates.getProjection();

        this.projectionSpec = new ProjectionSpecification(netcdfDataset,
            GridMapping.LAMBERT_CONFORMAL_PROJECTION);
        convertToLatLon();
    }

    public GridHandler(GridDataset gridDataset)
        throws GribCreationException, IOException {

        this.coordinates = getCoordinateSystem(gridDataset);
        this.projection = coordinates.getProjection();

        this.projectionSpec = new ProjectionSpecification(gridDataset.getNetcdfDataset(),
            GridMapping.LAMBERT_CONFORMAL_PROJECTION);
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
        for (int i = 0; i < projectionSpec.getyShape()[0]; i++) {
            for (int j = 0; j < projectionSpec.getxShape()[0]; j++) {
                double X = projectionSpec.getxData().getDouble(projectionSpec.getxIndex().set(j));
                double Y = projectionSpec.getyData().getDouble(projectionSpec.getyIndex().set(i));
                this.latitude.add(projection.projToLatLon(X, Y).getLatitude());
                this.longitude.add(projection.projToLatLon(X, Y).getLongitude());
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