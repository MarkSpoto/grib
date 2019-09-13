package io.phdata.tools.grib.core.common;

import io.phdata.tools.grib.core.util.FileUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import ucar.nc2.dt.GridCoordSystem;
import ucar.nc2.time.CalendarDate;

/**
 * Created by cisaksson on 8/14/19.
 */
public class ReferenceTimeHandler {
    private static final Log LOG = LogFactory.getLog(ReferenceTimeHandler.class);
    private static final String CF_TIME = "time";
    private static final String UNITS = "units";

    private VarMeta varMeta;
    private GridCoordSystem coordSystem;
    private List<CalendarDate> calendarDates;

    private int T;
    private int X;
    private int Y;

    public ReferenceTimeHandler(GridCoordSystem coordSystem, VarMeta varMeta, int t, int x, int y) {
        this.T = t;
        this.X = x;
        this.Y = y;
        this.varMeta = varMeta;
        this.coordSystem = coordSystem;
        this.calendarDates = getCalendarDates();
    }

    /**
     * This function process the valid time in UTC time zone.
     */
    private List<CalendarDate> getCalendarDates() {
        return this.coordSystem
            .getTimeAxis1D()
            .getCalendarDates();
    }

    /**
     * This function process the reference time
     */
    public List<String> getReferenceTime() {
        JSONObject json = new JSONObject(this.varMeta.getAttribute(CF_TIME));
        String refTime = FileUtil.toDate(json.get(UNITS).toString(), null);

        return Collections.nCopies(T * X * Y, refTime);
    }

    public List<String> getValidTime() {
        List<List<String>> validTimeUTC = new ArrayList<>();
        for (CalendarDate date : this.calendarDates) {
            List<String> arrays = Collections.nCopies(X * Y, date.toString());
            validTimeUTC.add(arrays);
        }
        return validTimeUTC.stream().collect(ArrayList::new, List::addAll, List::addAll);
    }
}