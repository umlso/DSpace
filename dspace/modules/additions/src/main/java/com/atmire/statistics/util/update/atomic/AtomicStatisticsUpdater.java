package com.atmire.statistics.util.update.atomic;

import com.atmire.modules.ModuleVersion;
import com.atmire.statistics.CUAConstants;
import com.atmire.statistics.util.update.atomic.processor.RecordVisitor;
import com.atmire.statistics.util.update.atomic.record.Record;
import com.atmire.statistics.util.update.atomic.record.RecordFactory;
import com.atmire.tracing.service.TracingService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.dspace.core.ConfigurationManager;
import org.dspace.core.Email;
import org.dspace.statistics.SolrLogger;
import org.joda.time.Period;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import javax.mail.MessagingException;
import java.io.IOException;
import java.util.*;

/**
 * Created by: Art Lowel (art dot lowel at atmire dot com)
 * Date: 16 06 2015
 */
public class AtomicStatisticsUpdater {
    private static final int CONSECUTIVE_STORE_ON_SERVER_ERROR_LIMIT = 50;
    private Logger log = Logger.getLogger(AtomicStatisticsUpdater.class);

    private Boolean resume = true;
    private String alertEmailAddress;
    private String targetCore;
    private Integer runTimeInHours;
    private Integer nbRowsPerQuery;
    private Integer run;
    private HttpSolrServer server = null;
    private Long nbRecordsWithProcessingError = 0l;
    private Long nbUpdatedRecords = 0l;
    private Long nbTotalRecords = null;
    private Integer nbConsecutiveStoreOnServerErrors = 0;
    private Calendar startCal;
    private Calendar endCal;
    private Calendar prevSplitCal;
    private boolean hasRun = false;

    @Autowired
    private List<RecordVisitor> processors;

    @Autowired
    private TracingService tracingService;

    @Resource(name="filterQueriesForAtomicUpdate")
    private List<String> springFilterQueries;


    @Resource(name="cuaVersionsThatRequireMigration")
    private List<String> cuaVersionsThatRequireMigration;

    public AtomicStatisticsUpdater(Boolean resume, Integer nbRowsPerQuery, String alertEmailAddress, String targetCore, Integer runTimeInHours) {
        configure(resume, nbRowsPerQuery, alertEmailAddress, targetCore, runTimeInHours);
    }

    public void configure(Boolean resume, Integer nbRowsPerQuery, String alertEmailAddress, String targetCore, Integer runTimeInHours) {
        if (nbRowsPerQuery != null) {
            this.nbRowsPerQuery = nbRowsPerQuery;
        }
        this.resume = resume;
        this.alertEmailAddress = alertEmailAddress;
        this.targetCore = targetCore;
        this.runTimeInHours = runTimeInHours;
    }

    //TODO separate out concerns (e.g. move progress logging to dedicated class)
    public void update() {
        startTimer();
        this.hasRun = true;


        List<String> coresToProcess;
        if (StringUtils.isBlank(targetCore)) {
            coresToProcess = SolrLogger.getStatisticYearCores();
        } else {
            coresToProcess = Collections.singletonList(targetCore);
        }

        try {
            determineRun();
            for (String core : coresToProcess) {
                targetCore = core;
                performRun();
            }
        } finally {
            for (String core : coresToProcess) {
                targetCore = core;
                commit();
                getSolrServer().shutdown();
            }
        }

        //TODO report on nb orphaned records after run (optionally with a suggested query to delete them all)
        stopTimer();
        logFinalDuration();
        logFailed();
    }

    private void performRun() {
        SolrDocumentList solrDocs;
        do {
            solrDocs = getNextSetOfSolrDocuments();
            List<Record> records = covertSolrDocsToRecords(solrDocs);

            processRecords(records);

            commit();
            logProgress();

        } while (thereAreMoreProcessedRecords(solrDocs) && !outOfTime());
    }

    private void processRecords(List<Record> records) {
        for (Record record : records) {
            try {
                applyProcessors(record);
                storeOnServer(record);
                nbUpdatedRecords++;
            } catch (ProcessingException e) {
                log.error("Record " + record + " couldn't be processed", e);
                nbRecordsWithProcessingError++;
            }
        }
    }

    public void logStarted() {
        log.info("\n\n" +
                "*************************\n" +
                "* Update Script Started *\n" +
                "*************************\n");
    }

    private void startTimer() {
        startCal = Calendar.getInstance();
        prevSplitCal = startCal;
    }

    public void logFinished() {
        log.info("\n\n" +
                "**************************\n" +
                "* Update Script Finished *\n" +
                "**************************\n");
    }

    private void logFinalDuration() {
        if (startCal != null && endCal != null) {
            String formatted = LogUtils.getFormattedDuration(startCal, endCal);
            log.info(getRunPrefix() + "took " + formatted);
        } else {
            log.warn(getRunPrefix() + "Couldn't determine duration");
        }
    }

    private void logFailed() {
        if (nbRecordsWithProcessingError > 0) {
            log.info(LogUtils.NUMBER_FORMATTER.format(nbRecordsWithProcessingError) + " docs failed to process\nIf run the update again with the resume option (-r) they will be reattempted");
        }
    }

    private void stopTimer() {
        endCal = Calendar.getInstance();
    }

    private void logProgress() {
        Integer percentage = calculatePercentage(nbUpdatedRecords, nbTotalRecords);
        Calendar now = Calendar.getInstance();
        String splitTime = LogUtils.getFormattedDuration(startCal, now);
        String lapTime = LogUtils.getFormattedDuration(prevSplitCal, now);
        prevSplitCal = now;

        log.info(getRunPrefix() + "— " + formatPercentage(percentage) +
                " — " + LogUtils.NUMBER_FORMATTER.format(nbUpdatedRecords) +
                "/" + LogUtils.NUMBER_FORMATTER.format(nbTotalRecords) + " docs" +
                " — " + lapTime + " — " + splitTime);

        tracingService.log(getRunPrefix()+"\n");
        tracingService.clear();
    }

    private String getRunPrefix() {
        return "Run " + run + " ";
    }

    private Integer calculatePercentage(Long value, Long total) {
        Integer percentage;
        if (total == 0) {
            percentage = 100;
        }
        else {
            percentage = (int) Math.round((double) value / (double) total * 100);
        }
        return percentage;
    }

    private String formatPercentage(Integer percentage) {
        String percentageStr = "";
        if (percentage < 100) {
            percentageStr += " ";
            if (percentage < 10) {
                percentageStr += " ";
            }
        }
        percentageStr += percentage + "%";
        return percentageStr;
    }

    protected void sendFinishedAlert(){
        if (StringUtils.isNotBlank(alertEmailAddress))
        {
            try {
                Email email = new Email();
                email.setSubject("CUA Statistics Update script for client: " + ConfigurationManager.getProperty("dspace.name"));
                if(this.hasRun) {
                    email.setContent("The CUA Statistics update script for client: " + ConfigurationManager.getProperty("dspace.name") + " has finished running.");
                } else {
                    email.setContent("The CUA Statistics update script for client: " + ConfigurationManager.getProperty("dspace.name") + " did not run.");
                }
                email.addRecipient(alertEmailAddress);
                email.send();
            } catch (MessagingException | IOException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private boolean thereAreMoreProcessedRecords(SolrDocumentList solrDocs) {
//        return false;
        return solrDocs != null && solrDocs.getNumFound() > solrDocs.size();
    }

    private void applyProcessors(Record record) throws ProcessingException {
        List<RecordVisitor> processors = getProcessors();
        for (RecordVisitor processor : processors) {
            record.accept(processor);
        }
    }

    private void storeOnServer(Record record) {
        /* This check is disabled to ensure all records get processed and
         * get missing fields that have defaultValues in schema.xml
         * automatically
         */
//        if (record.needsUpdate()) {
            try {
                getSolrServer().add(record.getTargetDocument());
                nbConsecutiveStoreOnServerErrors = 0;
            } catch (HttpSolrServer.RemoteSolrException | SolrServerException | IOException e) {
                log.error(record + " couldn't be saved. It will be re-attempted automatically later in this run", e);
                nbConsecutiveStoreOnServerErrors++;
                if (nbConsecutiveStoreOnServerErrors >= CONSECUTIVE_STORE_ON_SERVER_ERROR_LIMIT) {
                    throw new RuntimeException(CONSECUTIVE_STORE_ON_SERVER_ERROR_LIMIT + " consecutive records couldn't be saved. There's most likely an issue with the connection to the solr server. Shutting down.");
                }
            }
//        }
    }

    private List<RecordVisitor> getProcessors() {
        return processors;
    }

    private List<Record> covertSolrDocsToRecords(SolrDocumentList results) {
        List<Record> records;

        if (results != null && results.size() > 0) {
            records = new ArrayList<>(results.size());

            if (outOfTime()) {

            }

            for (SolrDocument doc : results) {
                Record record = RecordFactory.getInstance().createRecord(doc, run);
                records.add(record);
            }
        }
        else {
            records = new ArrayList<>(0);
        }

        return records;
    }

    private void commit() {
        try {
            getSolrServer().commit();
        } catch (SolrServerException | IOException e) {
            log.error("There was a problem with a solr commit", e);
        }
    }

    /**
     * The next set of solr docs is determined by searching for
     * docs that don't have the current module version and run number
     * in their fields
     */
    private SolrDocumentList getNextSetOfSolrDocuments() {
        try {
            SolrQuery query = new SolrQuery();
            query.setQuery("*:*");
            String exclude = docsAlreadyUpdatedInTheCurrentRun();
            if (resume) {
                exclude += " OR " + docsLoggedByThisCuaVersion(); // These are recent logs which are already up to date
            }
            String fq = String.format("-(%s)", exclude);
            query.add("fq", fq);
            addUserFilterQueries(query);
            query.setSort("id", SolrQuery.ORDER.desc);
            query.addSort("type", SolrQuery.ORDER.desc);
            query.setRows(nbRowsPerQuery);

            QueryResponse response = getSolrServer().query(query);
            SolrDocumentList results = response.getResults();
            if (nbTotalRecords == null) {
                nbTotalRecords = results.getNumFound();
            }
            return results;
        } catch (SolrServerException e) {
            throw new RuntimeException("There was an error querying the solr core for the next set of documents", e);
        }
    }

    // docs with the current cua version and the current run field
    private String docsAlreadyUpdatedInTheCurrentRun() {
        return "(" + CUAConstants.FIELD_CUA_VERSION + ":" + CUAConstants.MODULE_VERSION +
                " AND " + CUAConstants.FIELD_CORE_UPDATE_RUN_NB + ":" + run + ")";
    }

    // docs with the current cua version and no run field
    private String docsLoggedByThisCuaVersion() {
        return "(" + CUAConstants.FIELD_CUA_VERSION + ":" + CUAConstants.MODULE_VERSION +
                " AND -" + CUAConstants.FIELD_CORE_UPDATE_RUN_NB + ":[* TO *])";
    }

    private void logOrphanedRecords() {
        try {
            SolrQuery query = new SolrQuery();
            query.setQuery(CUAConstants.FIELD_CUA_VERSION + ":" + CUAConstants.MODULE_VERSION);
            addUserFilterQueries(query);
            query.set("fl", CUAConstants.FIELD_CUA_VERSION + ", " + CUAConstants.FIELD_CORE_UPDATE_RUN_NB);
            query.setSort(CUAConstants.FIELD_CORE_UPDATE_RUN_NB, SolrQuery.ORDER.desc);
            query.setRows(1);

            QueryResponse response = getSolrServer().query(query);
            SolrDocumentList results = response.getResults();
            if (results != null && results.size() > 0) {
                SolrDocument doc = results.get(0);
                Integer lastRun = (Integer) doc.getFieldValue(CUAConstants.FIELD_CORE_UPDATE_RUN_NB);
                if (resume) {
                    run = lastRun;
                }
                else {
                    run = lastRun + 1;
                }
            }
            else {
                run = 1;
            }
            log.info(getRunPrefix());
        } catch (SolrServerException e) {
            throw new RuntimeException("There was an error determining the run number", e);
        }
    }

    public void addLogAppender(Appender consoleAppender) {
        log.addAppender(consoleAppender);
    }

    private Migration requiresMigration(String statVersionString) {
        ModuleVersion statVersion = new ModuleVersion(statVersionString);

        ModuleVersion currentVersion = new ModuleVersion(CUAConstants.MODULE_VERSION);
        if (currentVersion.isBefore(statVersion)) {
            log.warn(statVersionString + " is a more recent CUA version than the current version " + CUAConstants.MODULE_VERSION);
            return Migration.UNDETERMINED;
        }

        for (String version : cuaVersionsThatRequireMigration) {
            ModuleVersion configVersion = new ModuleVersion(version);

            if(statVersion.isBefore(configVersion)
                    && configVersion.isBefore(currentVersion))
            {
                return Migration.NECESSARY;
            }
        }
        return Migration.UNNECESSARY;
    }

    public Migration shouldRun() {
        Migration migration = Migration.UNNECESSARY;
        Set<String> versions = currentStatVersions();

        StringBuilder versionReport = new StringBuilder();
        if (CollectionUtils.isNotEmpty(versions)) {
            versionReport.append("The current statistics have been added by the following cua versions:\n");
            for (String version : versions) {

                Migration versionMigration = requiresMigration(version);
                migration = migration.precedence(versionMigration);

                versionReport.append(version);
                if (versionMigration == Migration.UNNECESSARY) {
                    versionReport.append(" -> migration not necessary");
                }
                if (versionMigration == Migration.NECESSARY) {
                    versionReport.append(" -> requires migration");
                }
                if (versionMigration == Migration.UNDETERMINED) {
                    versionReport.append(" -> cannot determine the necessity of the migration");
                }
                versionReport.append("\n");
            }
        } else {
            versionReport.append("No prior cua versions have been found.\n");
            migration = Migration.NECESSARY;
        }

        versionReport.append("The migration check concluded ");
        if (migration == Migration.UNDETERMINED) {
            versionReport.append("the necessity of the migration is undetermined.");
        }
        if (migration == Migration.NECESSARY) {
            versionReport.append("the migration is necessary.");
        }
        if (migration == Migration.UNNECESSARY) {
            versionReport.append("the migration is not necessary.");
        }

        log.info(versionReport);

        return migration;
    }

    private Set<String> currentStatVersions() {
        Set<String> versions = new HashSet<>();
        String facetField = CUAConstants.FIELD_CUA_VERSION;
        SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.setRows(0);
        addUserFilterQueries(query);
        query.setFacet(true);
        query.addFacetField(facetField);

        if (StringUtils.isBlank(targetCore)) {
            SolrLogger.addAdditionalSolrYearCores(query);
        }

        QueryResponse response = null;
        try {
            response = getSolrServer().query(query);
        } catch (SolrServerException e) {
            throw new RuntimeException("There was an error checking if the migration is necessary.", e);
        }

        if (response != null) {
            FacetField responseFacetField = response.getFacetField(facetField);
            List<FacetField.Count> values = responseFacetField.getValues();
            for (FacetField.Count value : values) {
                String version = value.getName();
                versions.add(version);
            }
        }

        return versions;
    }

    private void determineRun() {
        try {
            SolrQuery query = new SolrQuery();
            query.setQuery(CUAConstants.FIELD_CUA_VERSION + ":" + CUAConstants.MODULE_VERSION);
            addUserFilterQueries(query);
            query.set("fl", CUAConstants.FIELD_CUA_VERSION + ", " + CUAConstants.FIELD_CORE_UPDATE_RUN_NB);
            query.setSort(CUAConstants.FIELD_CORE_UPDATE_RUN_NB, SolrQuery.ORDER.desc);
            query.setRows(1);

            if (StringUtils.isBlank(targetCore)) {
                SolrLogger.addAdditionalSolrYearCores(query);
            }

            QueryResponse response = getSolrServer().query(query);
            SolrDocumentList results = response.getResults();
            if (results != null && results.size() > 0) {
                SolrDocument doc = results.get(0);
                Integer lastRun = (Integer) doc.getFieldValue(CUAConstants.FIELD_CORE_UPDATE_RUN_NB);
                if (lastRun == null) {
                    run = 1;
                }
                else if (resume) {
                    run = lastRun;
                }
                else {
                    run = lastRun + 1;
                }
            }
            else {
                run = 1;
            }
            log.info(getRunPrefix());
        } catch (SolrServerException e) {
            throw new RuntimeException("There was an error determining the run number", e);
        }
    }

    private void addUserFilterQueries(SolrQuery query) {
        for (String fq : springFilterQueries) {
            query.add("fq", fq);
        }
    }

    private HttpSolrServer getSolrServer() {
        if (server == null) {
            server = new HttpSolrServer(StringUtils.isNotBlank(targetCore) ?
                    CUAConstants.STATS_CORE.replace("statistics",targetCore) : CUAConstants.STATS_CORE);
        }
        return server;
    }

    /**
     * Check if we have time left to run our script
     * @return true if we should quit executing our script
     */
    public boolean outOfTime()
    {
        return new Period(startCal.getTimeInMillis(), Calendar.getInstance().getTimeInMillis()).getHours()
                > runTimeInHours;
    }
}
