package io.phdata.tools.grib.core.preprocessing.store;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cisaksson on 8/12/19.
 */
public class GribFileContainer {
    private List<String> fileDates;
    private List<Double> latitude;
    private List<Double> longitude;
    private List<String> referenceTime;
    private Double[] temperature;
    private List<String> tempDescription;
    private List<String> validTime;

    public GribFileContainer() {
        this.fileDates = null;
        this.latitude = null;
        this.longitude = null;
        this.referenceTime = null;
        this.temperature = null;
        this.tempDescription = null;
        this.validTime = null;
    }

    public GribFileContainer(List<String> fileDates, List<Double> latitude,
        List<Double> longitude, List<String> referenceTime, Double[] temperature,
        List<String> tempDescription, List<String> validTime) {
        this.fileDates = fileDates;
        this.latitude = latitude;
        this.longitude = longitude;
        this.referenceTime = referenceTime;
        this.temperature = temperature;
        this.tempDescription = tempDescription;
        this.validTime = validTime;
    }

    public List<GribRow> getRows() {
        List<GribRow> data = new ArrayList<>();
        for (int i=0; i<latitude.size(); i++) {
            data.add(new GribRow(
                fileDates.get(i),
                latitude.get(i),
                longitude.get(i),
                referenceTime.get(i),
                temperature[i],
                tempDescription.get(i),
                validTime.get(i)
            ));
        }
        return data;
    }

    public List<String> getFileDates() {
        return fileDates;
    }

    public void setFileDates(List<String> fileDates) {
        this.fileDates = fileDates;
    }

    public List<Double> getLatitude() {
        return latitude;
    }

    public void setLatitude(List<Double> latitude) {
        this.latitude = latitude;
    }

    public List<Double> getLongitude() {
        return longitude;
    }

    public void setLongitude(List<Double> longitude) {
        this.longitude = longitude;
    }

    public List<String> getReferenceTime() {
        return referenceTime;
    }

    public void setReferenceTime(List<String> referenceTime) {
        this.referenceTime = referenceTime;
    }

    public Double[] getTemperature() {
        return temperature;
    }

    public void setTemperature(Double[] temperature) {
        this.temperature = temperature;
    }

    public List<String> getTempDescription() {
        return tempDescription;
    }

    public void setTempDescription(List<String> tempDescription) {
        this.tempDescription = tempDescription;
    }

    public List<String> getValidTime() {
        return validTime;
    }

    public void setValidTime(List<String> validTime) {
        this.validTime = validTime;
    }
}
