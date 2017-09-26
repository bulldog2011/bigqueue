package com.leansoft.bigqueue.utils;

public class BQSettings {
    /**
     * To control the file lastModified epoch update time.
     */
    public static final String LAST_MODIFIED_UPDATE_INTERVAL_SECOND = "last.modified.update.intervalSec";
    public static final int DEFAULT_LAST_MODIFIED_UPDATE_INTERVAL_SECOND = 5;
    public static long getLastModifiedUpdateInterval() {
        String value = System.getProperty(LAST_MODIFIED_UPDATE_INTERVAL_SECOND, "" + DEFAULT_LAST_MODIFIED_UPDATE_INTERVAL_SECOND);

        try {
            return Integer.parseInt(value) * 1000;
        }
        catch (Exception e) {
            return DEFAULT_LAST_MODIFIED_UPDATE_INTERVAL_SECOND * 1000;
        }
    }
}
