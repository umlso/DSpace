package com.atmire.statistics.mostpopular;

import com.atmire.statistics.display.StatisticsDataVisitsMultidata;
import org.apache.log4j.Logger;
import org.dspace.core.Context;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by Bavo Van Geit
 * Date: 19/05/14
 * Time: 12:40
 */
public class ReferrerDataProcessor implements DataProcessor {

    private static Logger log = Logger.getLogger(ReferrerDataProcessor.class);

    public String getRowLabel(Context context, StatisticsDataVisitsMultidata statisticsData, String raw) {
        return raw;
    }

    public Map<String, String> getRowLabelAttr(Context context, StatisticsDataVisitsMultidata statisticsData, String raw) {
//        return Collections.singletonMap("url", raw);
        return Collections.emptyMap();
    }

    public List<String> getAdditionalColumns() {
        return null;
    }

    public String getAdditionalColumnValue(Context context, int col, StatisticsDataVisitsMultidata statisticsData, String raw) {
        return null;
    }
}
