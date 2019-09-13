package io.phdata.tools.grib;

import io.phdata.tools.grib.core.preprocessing.GribFileBuilder;
import io.phdata.tools.grib.core.preprocessing.store.GribCreationException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;

public class GRIB2FileTest {

  public static void main(String[] args) throws Exception {

//    String file = "/Users/cisaksson/Downloads/A_HHCA92RJTD291200_C_RJTD_20190729153232_61.grib";
    String file = "/Users/cisaksson/Downloads/YGUZ97_KWBN_201902271132.grb2";

//    InputStream inputStream = new FileInputStream(file);

    GribFileBuilder builder = new GribFileBuilder(file);
//
//    System.out.println(builder.toDataFrame().print(100));
  }
}
