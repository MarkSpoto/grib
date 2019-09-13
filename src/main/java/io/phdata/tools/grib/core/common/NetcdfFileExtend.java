package io.phdata.tools.grib.core.common;

import java.io.IOException;
import ucar.nc2.NetcdfFile;
import ucar.unidata.io.RandomAccessFile;

/**
 * Created by cisaksson on 8/14/19.
 */
public class NetcdfFileExtend extends NetcdfFile {
    protected NetcdfFileExtend() {
    }

    public static NetcdfFile open(RandomAccessFile fileAccess, String location) throws IOException {
        try {
            return open(fileAccess, location, null, null);
        } catch (Throwable var6) {
            fileAccess.close();
            throw new IOException(var6);
        }
    }
}
