package io.phdata.tools.grib.core.preprocessing.store;

import java.io.Serializable;

/**
 * Created by cisaksson on 8/12/19.
 */
public class GribRow implements Serializable {
    private String file_time_utc;
    private Double latitude;
    private Double longitude;
    private String reference_time_utc;
    private Double temperature_kelvin;
    private String temperature_type;
    private String valid_time_utc;

    public GribRow(String file_time_utc, Double latitude, Double longitude,
        String reference_time_utc, Double temperature_kelvin, String temperature_type,
        String valid_time_utc) {
        this.file_time_utc = file_time_utc;
        this.latitude = latitude;
        this.longitude = longitude;
        this.reference_time_utc = reference_time_utc;
        this.temperature_kelvin = temperature_kelvin;
        this.temperature_type = temperature_type;
        this.valid_time_utc = valid_time_utc;
    }

    public String getFile_time_utc() {
        return file_time_utc;
    }

    public Double getLatitude() {
        return latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public String getReference_time_utc() {
        return reference_time_utc;
    }

    public Double getTemperature_kelvin() {
        return temperature_kelvin;
    }

    public String getTemperature_type() {
        return temperature_type;
    }

    public String getValid_time_utc() {
        return valid_time_utc;
    }
}
