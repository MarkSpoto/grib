package io.phdata.tools.grib.core.common;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import ucar.ma2.Array;
import ucar.nc2.dataset.NetcdfDataset;

/**
 * Created by cisaksson on 8/14/19.
 */
public class TemperatureHandler {
    private static final Log LOG = LogFactory.getLog(TemperatureHandler.class);
    private static final String CF_DESCRIPTION = "description";

    private NetcdfDataset netcdfDataset;
    private String description;
    private VarMeta varMeta;
    private int size;


    public TemperatureHandler(NetcdfDataset netcdfDataset, VarMeta varMeta, int size) {
        this.netcdfDataset = netcdfDataset;
        this.varMeta = varMeta;
        this.size = size;

        description = new JSONObject(varMeta.getAttribute(varMeta.getAttribute(VarMeta.GRID_ID)))
            .get(CF_DESCRIPTION).toString();
    }

    /**
     * This function process the temperature data
     */
    public Double[] getTemperatureVector() throws IOException {
        Array temperature = this.netcdfDataset
            .findVariable(varMeta.getAttribute(VarMeta.GRID_ID)).read();

        Double[] tempValues = new Double[Math.toIntExact(temperature.getSize())];
        for (int i=0; i<tempValues.length; i++) {
            tempValues[i] = temperature.getDouble(i);
        }
        return tempValues;
    }

    /**
     * This function add the description of the type of the temperature
     */
    public List<String> getTempDescription() {
        return Collections.nCopies(size, description);
    }
}