package io.phdata.tools.grib.core.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by cisaksson on 8/12/19.
 */
public class FileUtil {
    private static final Log LOG = LogFactory.getLog(FileUtil.class);

    public static String extractFileName(String fullPathFile) {
        try {
            Pattern regex = Pattern.compile("([^\\\\/:*?\"<>|\r\n]+$)");
            Matcher regexMatcher = regex.matcher(fullPathFile);
            if (regexMatcher.find()) {
                return regexMatcher.group(1);
            }
        } catch (PatternSyntaxException ex) {
            LOG.info("extractFileName::pattern problem <" + fullPathFile + ">", ex);
        }
        return fullPathFile;
    }

    public static String extractDate(String FilePath) {
        try {
            Pattern regex = Pattern.compile("_([0-9]+)_*");
            Matcher regexMatcher = regex.matcher(FilePath);
            if (regexMatcher.find()) {
                return regexMatcher.group(1);
            }
        } catch (PatternSyntaxException ex) {
            LOG.info("extractFileName::pattern problem <" + FilePath + ">", ex);
        }
        return FilePath;
    }

    public static String toDateTime(String time, String pattern) {
        if (pattern == null) {
            pattern = "yyyyMMddHHmmss";
        }
        try {
            DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern).withZoneUTC();
            time = formatter.parseDateTime(time).toString();
        } catch (IllegalArgumentException ex) {
            time = Instant.now().toString();
        }
        return time;
    }

    public static String toDate(String date, String pattern){
        if(pattern == null) {
            pattern = "(([\\.\\:?\\d]*)([\\+\\-]\\S*)?z?)?$";
        }
        Pattern regex = Pattern.compile(pattern);
        Matcher regexMatcher = regex.matcher(date);
        if (regexMatcher.find()) {
            date = regexMatcher.group(1);
        }
        return date;
    }
}
