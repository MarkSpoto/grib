package io.phdata.tools.grib.core.common;

import java.io.IOException;
import java.util.Arrays;
import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.dataset.NetcdfDataset;


/**
 * Created by cisaksson on 8/14/19.
 */
class ProjectionSpecification {
    private static final String X_AXIS_ID = "x";
    private static final String Y_AXIS_ID = "y";
    private static final String LAT_AXIS_ID = "lat";
    private static final String LON_AXIS_ID = "lon";

    private Array xData;
    private Array yData;
    private int[] xShape;
    private int[] yShape;
    private Index xIndex;
    private Index yIndex;

    public ProjectionSpecification(NetcdfDataset netcdfDataset, GridMapping mapping)
        throws IOException {
        switch (mapping) {
            case LAMBERT_CONFORMAL_PROJECTION:
                processProjection(netcdfDataset, X_AXIS_ID, Y_AXIS_ID);
                processShape();
                processIndex();
                break;
            case LATLON_PROJECTION:
                processProjection(netcdfDataset, LAT_AXIS_ID, LON_AXIS_ID);
                break;
            default:
                throw new IllegalArgumentException("Projection not defined");
        }
    }

    private void processProjection(NetcdfDataset netcdfDataset, String aAxis, String bAxis)
        throws IOException {
        this.xData = netcdfDataset.findVariable(aAxis).read();
        this.yData = netcdfDataset.findVariable(bAxis).read();
    }

    private void processShape() {
        this.xShape = this.xData.getShape();
        this.yShape = this.yData.getShape();
    }

    private void processIndex() {
        this.xIndex = this.xData.getIndex();
        this.yIndex = this.yData.getIndex();
    }

    public Array getxData() {
        return xData;
    }

    public void setxData(Array xData) {
        this.xData = xData;
    }

    public Array getyData() {
        return yData;
    }

    public void setyData(Array yData) {
        this.yData = yData;
    }

    public int[] getxShape() {
        return xShape;
    }

    public void setxShape(int[] xShape) {
        this.xShape = xShape;
    }

    public int[] getyShape() {
        return yShape;
    }

    public void setyShape(int[] yShape) {
        this.yShape = yShape;
    }

    public Index getxIndex() {
        return xIndex;
    }

    public void setxIndex(Index xIndex) {
        this.xIndex = xIndex;
    }

    public Index getyIndex() {
        return yIndex;
    }

    public void setyIndex(Index yIndex) {
        this.yIndex = yIndex;
    }

    @Override
    public String toString() {
        return "ProjectionSpecification{" +
            "xData=" + xData +
            ", yData=" + yData +
            ", xShape=" + Arrays.toString(xShape) +
            ", yShape=" + Arrays.toString(yShape) +
            ", xIndex=" + xIndex +
            ", yIndex=" + yIndex +
            '}';
    }
}
