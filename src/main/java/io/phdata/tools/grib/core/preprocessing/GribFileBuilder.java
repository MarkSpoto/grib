package io.phdata.tools.grib.core.preprocessing;

import io.phdata.tools.grib.core.common.DataHandler;
import io.phdata.tools.grib.core.common.EnumUtils;
import io.phdata.tools.grib.core.common.GridHandler;
import io.phdata.tools.grib.core.common.GridMapping;
import io.phdata.tools.grib.core.common.LatLonHandler;
import io.phdata.tools.grib.core.common.ReferenceTimeHandler;
import io.phdata.tools.grib.core.common.TemperatureHandler;
import io.phdata.tools.grib.core.common.VarMeta;
import io.phdata.tools.grib.core.preprocessing.store.GribCreationException;
import io.phdata.tools.grib.core.preprocessing.store.GribFileContainer;
import io.phdata.tools.grib.core.util.FileUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dt.GridCoordSystem;
import ucar.nc2.dt.grid.GridDataset;

/**
 * Created by cisaksson on 8/12/19.
 */
public class GribFileBuilder {
    private static final Log LOG = LogFactory.getLog(GribFileBuilder.class);

    private static final int Y_ID = 1;
    private static final int X_ID = 2;
    private static final int TIMES_ID = 0;
    private static final String SHAPE = "shape";
    private static final String GRID_MAPPING = "grid_mapping";
    private static final Function<String, GridMapping> GRID_MAP = EnumUtils
        .lookupMap(GridMapping.class, GridMapping::getName);

    private VarMeta varMeta;
    private NetcdfDataset netcdfDataset;
    private GribFileContainer container;

    private int Y_DIM;
    private int X_DIM;
    private int TIMES_DIM;

    public GribFileBuilder(String datasetPath, String gribFileName) throws Exception {
        container = new GribFileContainer();
        GridDataset dataset = GridDataset.open(datasetPath);

        netcdfDataset = dataset.getNetcdfDataset();
        varMeta = new VarMeta(netcdfDataset);

        String gridId = varMeta.getAttribute(VarMeta.GRID_ID);
        JSONObject gridJson = new JSONObject(varMeta.getAttribute(gridId));
        String[] dim = gridJson.get(SHAPE).toString().split(" ");
        this.TIMES_DIM = Integer.parseInt(dim[TIMES_ID]);

        this.Y_DIM = Integer.parseInt(dim[Y_ID]);
        this.X_DIM = Integer.parseInt(dim[X_ID]);

        container.setFileDates(getFileTimestamp(gribFileName));

        process(gridJson);
    }

    private void process(JSONObject gridJson) throws GribCreationException, IOException {
        String mapping = gridJson.get(GRID_MAPPING).toString();
        DataHandler gribDataHandler;
        switch (GRID_MAP.apply(mapping.trim())) {
            case LATLON_PROJECTION:
                gribDataHandler = new LatLonHandler(this.netcdfDataset);
                break;
            case LAMBERT_CONFORMAL_PROJECTION:
                gribDataHandler = new GridHandler(this.netcdfDataset);
                break;
            default:
                throw new GribCreationException("Grib format not supported");
        }

        boolean needRepeat = TIMES_DIM > 1;
        List<Double> latitude = gribDataHandler.getLatitude();
        List<Double> longitude = gribDataHandler.getLongitude();
        if (needRepeat) {
            List<List<Double>> latitudeAll = new ArrayList<>();
            List<List<Double>> longitudeAll = new ArrayList<>();
            for (int i = 0; i < TIMES_DIM; i++) {
                latitudeAll.add(latitude);
                longitudeAll.add(longitude);
            }
            latitude = latitudeAll.stream().collect(ArrayList::new, List::addAll, List::addAll);
            longitude = longitudeAll.stream().collect(ArrayList::new, List::addAll, List::addAll);
        }
        container.setLatitude(latitude);
        container.setLongitude(longitude);

        GridCoordSystem coordSystem = gribDataHandler.getCoordinates();
        ReferenceTimeHandler timeHandler =
            new ReferenceTimeHandler(coordSystem, varMeta, TIMES_DIM, X_DIM, Y_DIM);
        container.setReferenceTime(timeHandler.getReferenceTime());
        container.setValidTime(timeHandler.getValidTime());

        TemperatureHandler temperatureHandler =
            new TemperatureHandler(netcdfDataset, varMeta, TIMES_DIM*Y_DIM*X_DIM);

        container.setTemperature(temperatureHandler.getTemperatureVector());
        container.setTempDescription(temperatureHandler.getTempDescription());
    }

    /**
     * This function add the file date time.
     * @return date and time from the Grib file
     */
    private List<String> getFileTimestamp(String file) {
        String fileName = FileUtil.extractFileName(file);
        String date = FileUtil.extractDate(fileName);
        String fileTimeUtc = FileUtil.toDateTime(date, "yyyyMMddHHmmss");
        return Collections.nCopies(TIMES_DIM*X_DIM*Y_DIM, fileTimeUtc);
    }

    public GribFileContainer getContainer() {
        return this.container;
    }

    public Table toDataFrame() {
        Table finalTable = Table.create("");

        StringColumn fileTimeColumn =
            StringColumn.create("file_time_utc", container.getFileDates());
        DoubleColumn latitudeColumn =
            DoubleColumn.create("latitude", container.getLatitude().toArray(new Double[0]));
        DoubleColumn longitudeColumn =
            DoubleColumn.create("longitude", container.getLongitude().toArray(new Double[0]));
        StringColumn referenceTimeColumn =
            StringColumn.create("reference_time_utc", container.getReferenceTime());
        DoubleColumn tempValuesColumn =
            DoubleColumn.create("temperature_kelvin", container.getTemperature());
        StringColumn temperatureTypeColumn =
            StringColumn.create("temperature_type", container.getTempDescription());
        StringColumn validTimeColumn =
            StringColumn.create("valid_time_utc", container.getValidTime());


        finalTable.addColumns(fileTimeColumn);
        finalTable.addColumns(latitudeColumn);
        finalTable.addColumns(longitudeColumn);
        finalTable.addColumns(referenceTimeColumn);
        finalTable.addColumns(tempValuesColumn);
         finalTable.addColumns(temperatureTypeColumn);
        finalTable.addColumns(validTimeColumn);

        return finalTable;
    }
}
