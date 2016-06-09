package com.atmire.statistics.display;

import com.atmire.statistics.AbstractDataset;
import com.atmire.statistics.content.DatasetQuery;
import com.atmire.statistics.content.filter.StatisticsHitTypeFilter;
import com.atmire.statistics.content.filter.mostpopular.StatisticsMostPopularFilter;
import com.atmire.statistics.generator.TopNDSODatasetGenerator;
import com.atmire.statistics.mostpopular.DataProcessor;
import com.atmire.statistics.mostpopular.JSonStatsParameters;
import com.atmire.statistics.params.SolrLoggerParametersBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.dspace.content.DSpaceObject;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.statistics.Dataset;
import org.dspace.statistics.ObjectCount;
import org.dspace.statistics.SolrLogger;
import org.dspace.statistics.content.DatasetGenerator;
import org.dspace.statistics.content.DatasetTypeGenerator;
import org.dspace.statistics.content.StatisticsDataVisits;
import org.dspace.statistics.content.filter.StatisticsFilter;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Roeland Dillen (roeland at atmire dot com)
 * Date: 07/03/13
 * Time: 10:04
 */
public class StatisticsDataVisitsMultidata extends StatisticsDataVisits {


    private StatisticsHitTypeFilter initialHitTypeFilter[] = new StatisticsHitTypeFilter[]{new StatisticsHitTypeFilter(new int[]{Constants.BITSTREAM}), new StatisticsHitTypeFilter(new int[]{Constants.ITEM}), new StatisticsHitTypeFilter(new int[]{Constants.BITSTREAM, Constants.ITEM})};
    private int column = 0;
    private int count;
    private StatisticsMostPopularFilter filter;
    private DataProcessor dataProcessor ;
    private String collection;
    private JSonStatsParameters parameters;

    public JSonStatsParameters getParameters() {
        return parameters;
    }

    /**
     * Construct a completely uninitialized query.
     */
    public StatisticsDataVisitsMultidata(int column, int count, StatisticsMostPopularFilter filter, DataProcessor dataProcessor, DSpaceObject dso) {
        super(dso);
        this.column = column;
        this.count = count;
        this.filter = filter;
        this.dataProcessor = dataProcessor;
        this.parameters = new JSonStatsParameters(count,null,filter,dataProcessor,column,-1,-1,null, Collections.<String,String>emptyMap(), null,null,null,null,null, Collections.<StatisticsFilter>emptyList(), null);
    }

    public StatisticsDataVisitsMultidata(int column, int count, DatasetGenerator generator, StatisticsMostPopularFilter filter, DataProcessor dataProcessor) {
        this(column,count, generator,filter, dataProcessor, null);
    }

    public StatisticsDataVisitsMultidata(JSonStatsParameters parameters){
        this.column = parameters.getColumn();
        this.count = parameters.getNbitems();
        addDatasetGenerator(parameters.getGenerator());
        this.filter = parameters.getFilter();
        this.dataProcessor = parameters.getDataProcessor();

        if(StringUtils.isNotBlank(parameters.getColl()))
        {
            this.collection = parameters.getColl();
        }
        this.parameters=parameters;

    }

    public StatisticsDataVisitsMultidata(int column, int count, DatasetGenerator generator, StatisticsMostPopularFilter filter, DataProcessor dataProcessor, String collection) {
        this(new JSonStatsParameters(count,generator,filter,dataProcessor,column,-1,-1,null, Collections.<String,String>emptyMap(), collection,null,null,null,null, Collections.<StatisticsFilter>emptyList(), null));
    }

    /**
     * Run the accumulated query and return its results.
     */
    @Override
    public AbstractDataset createDataset(Context context) throws SQLException, SolrServerException, ParseException {
        if (getDataset() != null) {
            return getDataset();
        } else {

            List<StatisticsFilter> filters = new LinkedList<>(getFilters());
            filters.add(initialHitTypeFilter[column]);
            List<String> filterQuery = getString(filters,context);
            DatasetQuery q;
            DatasetGenerator generator = getDatasetGenerators().get(0);

            if(generator instanceof DatasetTypeGenerator){
                DatasetTypeGenerator typeGenerator = ((DatasetTypeGenerator) generator);
                q = typeGenerator.toDatasetQuery();
            }else if (generator instanceof TopNDSODatasetGenerator){
                q = ((TopNDSODatasetGenerator) generator).toDatasetQuery(context);
            }
            else{
                TopNDSODatasetGenerator tndg = new TopNDSODatasetGenerator(Constants.ITEM, count);
                q = tndg.toDatasetQuery(context);
            }

            int max = q.getMax();
            q.setMax(max + 1);
            ObjectCount counts[] = getObjectCounts(filterQuery, q);
            q.setMax(max);

            boolean hasMore = false;
            if (counts.length > max + 1) {
                hasMore = true;
                counts = ArrayUtils.remove(counts, max);
            }

            Dataset dataset = new Dataset(counts.length, 3 + (dataProcessor.getAdditionalColumns() != null ? dataProcessor.getAdditionalColumns().size() : 0) );
            dataset.setHasMore(hasMore);
            final LinkedHashMap<String, Integer> scope = new LinkedHashMap<String, Integer>(counts.length);
            int i = 0;
            for (ObjectCount count : counts) {

                dataset.addValueToMatrix(i, column, count.getCount());
                dataset.setCellAttr(i, column, "type","number");

                scope.put(count.getValue(), i);
                if (count.getValue().toLowerCase().contains("total")) {
                    dataset.setRowLabel(i, count.getValue());
                    dataset.setRowLabelAttr(i, Collections.singletonMap("isTotal", "true"));
                } else {
                    dataset.setRowLabel(i, dataProcessor.getRowLabel(context,this,count.getValue()));
                    dataset.setRowLabelAttr(i, dataProcessor.getRowLabelAttr(context,this,count.getValue()));
                    for(int j = 0; j < (dataProcessor.getAdditionalColumns() != null ? dataProcessor.getAdditionalColumns().size() : 0);j++  ) {
                        dataset.addValueToMatrix(i, 3 + j, dataProcessor.getAdditionalColumnValue(context, j, this, count.getValue()));
                    }
                }
                i++;

            }
            if(scope.size()>0) {
                filter.setScope(scope);
            }
            for (int j = 0; j < initialHitTypeFilter.length; j++) {
                if (j == column) continue;
                filters = new LinkedList(getFilters());
                if (initialHitTypeFilter[j].getTypes().length > 0) {
                    filters.add(initialHitTypeFilter[j]);
                }
                if (scope.size() > 0) {
                    filters.add(filter);
                }
                counts = getObjectCounts(getString(filters, context), q);
                for (ObjectCount count : counts) {
                    if (scope.containsKey(count.getValue())) {
                        dataset.addValueToMatrix(scope.get(count.getValue()), j, count.getCount());
                        dataset.setCellAttr(scope.get(count.getValue()), j, "type", "number");
                    }
                }
                if(scope.containsKey("total")) {
                    filters = new LinkedList(getFilters());
                    if (initialHitTypeFilter[j].getTypes().length > 0) {
                        filters.add(initialHitTypeFilter[j]);
                    }
                    counts = getObjectCounts(getString(filters, context), q);
                    for (ObjectCount count : counts) {
                        if ("total".equalsIgnoreCase(count.getValue())) {
                            dataset.addValueToMatrix(scope.get(count.getValue()), j, count.getCount());
                            dataset.setCellAttr(scope.get(count.getValue()), j, "type", "number");
                        }
                    }
                }
            }

            return dataset;
        }
    }

    private ObjectCount[] getObjectCounts(List<String> filterQuery, DatasetQuery q) throws SolrServerException {
//            ObjectCount counts[] = SolrLogger.queryFacetField("*:*", filterQuery, q.getFacetField(), q.getMax(), true, null);
        return SolrLogger.queryFacetField(
                new SolrLoggerParametersBuilder()
                        .withApplyDefaultBotFilter(!getParameters().getParameters().containsKey("includeBots"))
                        .withMethod(SolrRequest.METHOD.POST)
                        .withRows(0)
                        .withFilterQuery(filterQuery)
                        .withFacetFields(Collections.singletonList(q.getFacetField()))
                        .withFacetSort(true)
                        .withFacetMinCount(parameters.getParameters().containsKey("includeZero") ? 0 : 1)
                        .withMax(q.getMax())
                        .create(),
                true);
    }

    private List<String> getString(List<StatisticsFilter> filters, Context context) {
        List<String>  filterQuery=new LinkedList<String>();
        for (StatisticsFilter statisticsFilter : filters) {
            filterQuery.add(statisticsFilter.toQuery(context));
        }
        return filterQuery;
    }




    public int getColumn() {
        return column;
    }

    public int getCount() {
        return count;
    }

    public String getCollection() {
        return collection;
    }



    public StatisticsHitTypeFilter[] getInitialHitTypeFilter() {
        return initialHitTypeFilter;
    }




}
