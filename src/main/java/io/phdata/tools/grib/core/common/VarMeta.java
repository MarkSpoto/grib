package io.phdata.tools.grib.core.common;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import ucar.nc2.Attribute;
import ucar.nc2.Variable;
import ucar.nc2.VariableSimpleIF;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dt.grid.GridDataset;


/**
 * Created by cisaksson on 8/14/19.
 */
public class VarMeta {
    private static final Log LOG = LogFactory.getLog(VarMeta.class);
    public static final String GRID_ID = "GridId";

    private Map<String, String> attributeMap;

    /**
     * Instantiates a new Var meta.
     *
     * @param netcdfDataset the attribute list
     */
    public VarMeta(NetcdfDataset netcdfDataset) {
        this.attributeMap = initialize(netcdfDataset);
    }

    /**
     * Gets attribute Map.
     *
     * @return the attribute Map
     */
    public Map<String, String> getAttributeMap() {
        return this.attributeMap;
    }

    /**
     * Sets attribute map.
     *
     * @param attributeMap the attribute map
     */
    public void setAttributeList(Map<String, String> attributeMap) {
        this.attributeMap = attributeMap;
    }

    /**
     * Initialize list.
     *
     * @return the list
     */
    private Map<String, String> initialize(NetcdfDataset netcdfDataset) {
        GridDataset dataset = null;
        try {
            dataset = new GridDataset(netcdfDataset);
        } catch (IOException ex) {
            LOG.error("Unable to retrieve the grid dataset");
        }
//    finalTable.write().csv("/Users/cisaksson/Downloads/grib_file.csv");

        Map<String, String> attributesMap = null;
        if(dataset != null) {
            attributesMap = new HashMap<>();
            for (Variable variable : netcdfDataset.getVariables()) {
                JSONObject jsonString = new JSONObject();
                for (Attribute attribute : variable.getAttributeContainer().getAttributes()) {
                    jsonString
                        .put(attribute.getShortName(), attribute.getValues().toString().trim());
                }

                jsonString.put("dims", variable.getDimensionsString());
                jsonString.put("shape", Arrays.toString(variable.getShape()).replaceAll("\\[|\\]|,", ""));

                attributesMap.put(variable.getShortName(), jsonString.toString());
            }
            VariableSimpleIF variable = dataset.getDataVariables().iterator().next();
            attributesMap.put(GRID_ID, variable.getShortName());
        }
        return attributesMap;
    }

    /**
     * Gets attribute.
     *
     * @param attributeName the attribute name
     * @return the attribute
     */
    public String getAttribute(String attributeName) {
        if(this.attributeMap.containsKey(attributeName)) {
            return this.attributeMap.get(attributeName);
        }
        LOG.error(
            "Do not find the attribute according to the specified attribute name: "
                + attributeName);
        return null;
    }

    /**
     * Has attribute boolean.
     *
     * @param attributeName the attribute name
     * @return the boolean
     */
    public boolean hasAttribute(String attributeName) {
        return this.attributeMap.containsKey(attributeName);
    }
}
