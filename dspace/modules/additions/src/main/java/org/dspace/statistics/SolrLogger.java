/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.statistics;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import com.atmire.dspace.statistics.decorators.DecoratorManager;
import com.atmire.features.NoAutowireHelper;
import com.atmire.statistics.CUAConstants;
import com.atmire.statistics.MetadataStorageInfo;
import com.atmire.statistics.MetadataStorageInfoService;
import com.atmire.statistics.SolrLogThread;
import com.atmire.statistics.operations.solr.components.update.*;
import com.atmire.statistics.params.SolrLoggerParameters;
import com.atmire.statistics.params.SolrLoggerParametersBuilder;
import com.atmire.statistics.params.SolrLoggerParams;
import com.atmire.statistics.plugins.SolrLoggerLogPlugin;
import com.atmire.statistics.plugins.metadata.MetadataInclusionInterface;
import com.atmire.statistics.util.update.csv.SolrCsvNewFieldProcessor;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.response.*;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.luke.FieldFlag;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.util.DateMathParser;
import org.dspace.content.*;
import org.dspace.content.Collection;
import org.dspace.core.ConfigurationManager;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.discovery.SearchServiceException;
import org.dspace.eperson.CUAGroupUtils;
import org.dspace.eperson.EPerson;
import org.dspace.eperson.Group;
import org.dspace.statistics.util.DnsLookup;
import org.dspace.statistics.util.LocationUtils;
import org.dspace.statistics.util.SpiderDetector;
import org.dspace.usage.UsageWorkflowEvent;
import org.dspace.utils.DSpace;
import org.dspace.versioning.VersionHistory;
import org.dspace.versioning.VersioningService;

import javax.servlet.http.HttpServletRequest;
import java.io.*;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Static holder for a HttpSolrClient connection pool to issue
 * usage logging events to Solr from DSpace libraries, and some static query
 * composers.
 * 
 * @author ben at atmire.com
 * @author kevinvandevelde at atmire.com
 * @author mdiggory at atmire.com
 */
public class SolrLogger {

	private static Logger log = Logger.getLogger(SolrLogger.class);
	
    private static final HttpSolrServer solr;

    public static final String DATE_FORMAT_8601 = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    public static final String DATE_FORMAT_DCDATE = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    private static final LookupService locationService;

    private static final boolean useProxies;

	private static List<MetadataStorageInfo> metadataStorageInfoList;

    private static volatile int count = 0;
    private static int commit = 10;
    private static ConcurrentLinkedQueue<SolrQuery> queryStack = new ConcurrentLinkedQueue<SolrQuery>();
    /* A counter keeping track of how many queries where performed successfully */
    private static int succeededQueries = 0;
    private static StartupSolrResponseThread waitWithStart;

    public static boolean canDisplaySolr = false;
	private static final boolean MULTITHREADED = ConfigurationManager.getBooleanProperty("atmire-cua", "solr.log.multithreaded", true);
    private static List<String> statisticYearCores = new ArrayList<String>();
    private static Map<String, HttpSolrServer> statisticYearCoresMap = new HashMap<>();

    public static List<String> getStatisticYearCores() {
        return statisticYearCores;
    }

	private static boolean disableEnum = false;

	public static boolean isDisableEnum() {
		return disableEnum;
	}

	public static void setDisableEnum(boolean disableEnum) {
		log.error("This method is a hack, are you sure?");
		SolrLogger.disableEnum = disableEnum;
	}

    public static enum StatisticsType {
   		VIEW ("view"),
   		SEARCH ("search"),
   		SEARCH_RESULT ("search_result"),
        WORKFLOW("workflow"),
        CONTENT("content");

   		private final String text;

        StatisticsType(String text) {
   	        this.text = text;
   	    }

		public String text() {
			return text;
    	}
	}

    static
    {
        log.info("solr-statistics.spidersfile:" + ConfigurationManager.getProperty("solr-statistics", "spidersfile"));
        log.info("solr-statistics.server:" + ConfigurationManager.getProperty("solr-statistics", "server"));
        log.info("usage-statistics.dbfile:" + ConfigurationManager.getProperty("usage-statistics", "dbfile"));
    	
        HttpSolrServer server = null;
        
        if (ConfigurationManager.getProperty("solr-statistics", "server") != null)
        {
            try
            {
                server = new HttpSolrServer(ConfigurationManager.getProperty("solr-statistics", "server"));
                SolrQuery solrQuery = new SolrQuery()
                        .setQuery("type:2 AND id:1");
                server.query(solrQuery);

                //Attempt to retrieve all the statistic year cores
                File solrDir = new File(ConfigurationManager.getProperty("dspace.dir") + "/solr/");
                File[] solrCoreFiles = solrDir.listFiles(new FileFilter() {

                    @Override
                    public boolean accept(File file) {
                        //Core name example: statistics-2008
                        return file.getName().matches("statistics-\\d\\d\\d\\d");
                    }
                });
                //Base url should like : http://localhost:{port.number}/solr
                String baseSolrUrl = server.getBaseURL().replace("statistics", "");
                for (File solrCoreFile : solrCoreFiles) {
                    log.info("Loading core with name: " + solrCoreFile.getName());

                    HttpSolrServer core = createCore(server, solrCoreFile.getName());
                    //Add it to our cores list so we can query it !
                    String coreName = baseSolrUrl.replace("http://", "").replace("https://", "") + solrCoreFile.getName();
                    statisticYearCores.add(coreName);
                    statisticYearCoresMap.put(coreName, core);
                }
                //Also add the core containing the current year !
                String coreName = server.getBaseURL().replace("http://", "").replace("https://", "");
                statisticYearCores.add(coreName);
                statisticYearCoresMap.put(coreName, server);
            } catch (Exception e) {
            	log.error(e.getMessage(), e);
            }
        }
        solr = server;

        // Read in the file so we don't have to do it all the time
        //spiderIps = SpiderDetector.getSpiderIpAddresses();

        LookupService service = null;
        // Get the db file for the location
        String dbfile = ConfigurationManager.getProperty("usage-statistics", "dbfile");
        if (dbfile != null)
        {
            try
            {
                service = new LookupService(dbfile,
                        LookupService.GEOIP_STANDARD);
            }
            catch (FileNotFoundException fe)
            {
                log.error("The GeoLite Database file is missing (" + dbfile + ")! Solr Statistics cannot generate location based reports! Please see the DSpace installation instructions for instructions to install this file.", fe);
            }
            catch (IOException e)
            {
                log.error("Unable to load GeoLite Database file (" + dbfile + ")! You may need to reinstall it. See the DSpace installation instructions for more details.", e);
            }
        }
        else
        {
            log.error("The required 'dbfile' configuration is missing in solr-statistics.cfg!");
        }
        locationService = service;

        if ("true".equals(ConfigurationManager.getProperty("useProxies")))
        {
            useProxies = true;
        }
        else
        {
            useProxies = false;
        }

        log.info("useProxies=" + useProxies);

        metadataStorageInfoList = MetadataStorageInfoService.getInstance().getMetadataStorageInfos();

        if (ConfigurationManager.getProperty("atmire-cua", "solr.statistics.interval") != null)
            commit = ConfigurationManager.getIntProperty("atmire-cua", "solr.statistics.interval");

        if (ConfigurationManager.getIntProperty("atmire-cua", "solr.statistics.timeout") > 0)
            solr.setSoTimeout(ConfigurationManager.getIntProperty("atmire-cua", "solr.statistics.timeout"));

        if (ConfigurationManager.getProperty("atmire-cua", "solr.statistics.startup.query-success-count") != null && ConfigurationManager.getProperty("atmire-cua", "solr.statistics.startup.max-response-time") != null) {
            waitWithStart = new StartupSolrResponseThread();
            waitWithStart.start();
        } else {
            canDisplaySolr = true;
        }
    }

    public interface ShardVisitor {
        void visit(HttpSolrServer solr);
    }

    public static void visitEachStatisticShard(ShardVisitor shardVisitor) {
        for (HttpSolrServer httpSolrServer : statisticYearCoresMap.values()) {
            shardVisitor.visit(httpSolrServer);
        }
    }

    /**
     * Old post method, use the new postview method instead !
     *
     * @deprecated
     * @param dspaceObject the object used.
     * @param request the current request context.
     * @param currentUser the current session's user.
     */
    public static void post(DSpaceObject dspaceObject, HttpServletRequest request,
            EPerson currentUser)
    {
        postView(dspaceObject, request,  currentUser);
    }

    /**
     * Store a usage event into Solr.
     *
     * @param dspaceObject the object used.
     * @param request the current request context.
     * @param currentUser the current session's user.
     */
    public static void postView(DSpaceObject dspaceObject, HttpServletRequest request,
                                EPerson currentUser)
    {
        if (solr == null || locationService == null)
        {
            return;
        }


        try
        {
            SolrInputDocument doc1 = getCommonSolrDoc(dspaceObject, request, currentUser);
            if (doc1 == null) return;

            if (dspaceObject instanceof Item)
            {
                Item item = (Item) dspaceObject;

                Context context = (Context) request.getAttribute("dspace.context");

                if (context != null)
                {
                    VersionHistory history = addVersioning(context, doc1, item);
                    if(history!=null && NoAutowireHelper.getFF4JInstance().check("cua-versioning")){
                        item = history.getLatestVersion().getItem();
                    }
                }

                // Store the metadata
				addMetadata(doc1, item);

            }

			if (dspaceObject instanceof Bitstream) {
				Bitstream bit = (Bitstream) dspaceObject;
				DSpaceObject parent = bit.getParentObject();
				if (parent.getType() == Constants.ITEM) {

                    Context context = (Context) request.getAttribute("dspace.context");

                    if (context != null)
                    {
                        VersionHistory history = addVersioning(context, doc1, bit, (Item) parent);
                        if(history!=null && NoAutowireHelper.getFF4JInstance().check("cua-versioning")){
                            parent = history.getLatestVersion().getItem();
                        }
                    }

					// Store the metadata
					addMetadata(doc1, (Item) parent);
                }
            }

            DecoratorManager manager = DecoratorManager.getInstance();
            if(manager!=null)
                manager.decorate(dspaceObject,doc1, StatisticsType.VIEW.text());


            doc1.addField("statistics_type", StatisticsType.VIEW.text());


            //commits are executed automatically using the solr autocommit
//            solr.commit(false, false);
            SolrLogThread logThread = new SolrLogThread(solr, doc1);
            logThread.start();
            if (!MULTITHREADED)
                logThread.join();


        }
        catch (RuntimeException re)
        {
            throw re;
        }
        catch (Exception e)
        {
        	log.error(e.getMessage(), e);
        }
    }

    private static VersionHistory addVersioning(Context context, SolrInputDocument doc1, Item item) {
        VersionHistory history = retrieveVersionHistory(context, item);
        if(history != null)
        {
            doc1.addField("version_id", "v"+history.getVersionHistoryId());
        }else{
            doc1.addField("version_id", "i"+item.getID());
        }
        return history;
    }

    private static VersionHistory addVersioning(Context context, SolrInputDocument doc1, Bitstream bitstream, Item parent) throws SQLException {
        VersionHistory history = retrieveVersionHistory(context, parent);
        String versionId = "i"+parent.getID();
        String fileId = bitstream.getInternalId();
        if (history != null) {
            versionId = "v"+history.getVersionHistoryId();
        }
        doc1.addField("version_id", versionId);
        doc1.addField("file_id", "f"+fileId);
        return history;
    }

    public static VersionHistory retrieveVersionHistory(Context c, Item item) {
        VersioningService versioningService = new DSpace().getSingletonService(VersioningService.class);
        return versioningService.findVersionHistory(c, item.getID());
    }

    public static void addMetadata(SolrInputDocument doc, Item item) {
        for (MetadataStorageInfo metadataStorageInfo : metadataStorageInfoList) {
            for (String metadataField : metadataStorageInfo.getMetadataFields()) {
                if (metadataField.startsWith("java.")) {

                    try {
                        MetadataInclusionInterface inclusion = (MetadataInclusionInterface) Class.forName(metadataField.replace("java.", "")).getConstructor().newInstance();
                        inclusion.addMetadata(doc, item);
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.error(e.getMessage(), e);
                    }
                } else {
                    String[] dcField = metadataField.split("\\.");


                    Metadatum[] vals = item.getMetadata(dcField[0],
                            dcField[1], dcField.length > 2 ? (dcField[2].equals("*") ? Item.ANY : dcField[2]) : null, Item.ANY);
                    for (Metadatum val1 : vals) {
                        String val = val1.value.replaceAll("\\p{C}", "?");
                        if(StringUtils.isNotBlank(val)){
                            doc.addField(String.valueOf(metadataStorageInfo.getSolrStatisticsField()) + "_mtdt", val);
                            doc.addField(metadataStorageInfo.getSolrStatisticsField() + "_mtdt_search", val.toLowerCase());
                        }
                    }
                }
            }
        }
    }

    /**
     * Returns a solr input document containing common information about the statistics
     * regardless if we are logging a search or a view of a DSpace object
     * @param dspaceObject the object used.
     * @param request the current request context.
     * @param currentUser the current session's user.
     * @return a solr input document
     * @throws SQLException in case of a database exception
     */
    private static SolrInputDocument getCommonSolrDoc(DSpaceObject dspaceObject, HttpServletRequest request, EPerson currentUser) throws SQLException {
		boolean isSpiderBot = request != null && SpiderDetector.isSpider(request);
        if(isSpiderBot &&
                !ConfigurationManager.getBooleanProperty("usage-statistics", "logBots", true))
        {
            return null;
        }

            SolrInputDocument doc1 = new SolrInputDocument();
            // Save our basic info that we already have

        if(request != null){
            String ip = request.getRemoteAddr();

	        if(isUseProxies() && request.getHeader("X-Forwarded-For") != null)
            {
                /* This header is a comma delimited list */
                String[] split = request.getHeader("X-Forwarded-For").split(",");
                for (int i = split.length - 1; i >= 0; i--) {
                    String xfip = split[i];
                    /* proxy itself will sometime populate this header with the same value in
                        remote address. ordering in spec is vague, we'll just take the last
                        not equal to the proxy
                    */
                    if (!request.getHeader("X-Forwarded-For").contains(ip)) {
                        ip = xfip.trim();
                    }
                }
	        }

            doc1.addField("ip", ip);

            //Also store the referrer
            if(request.getHeader("referer") != null){
                doc1.addField("referrer", request.getHeader("referer"));
            }

            try
            {
            	String dns = DnsLookup.reverseDns(ip);
                doc1.addField("dns", dns.toLowerCase());
            }
            catch (Exception e)
            {
    			log.error("Failed DNS Lookup for IP:" + ip);
                log.debug(e.getMessage(),e);
    		}
            
            // Save the location information if valid, save the event without
            // location information if not valid
            if(locationService != null)
            {
                Location location = locationService.getLocation(ip);
                if (LocationUtils.isValidLocation(location))
                {
                    String countryCode = location.countryCode;
                    doc1.addField("geoipcountrycode", countryCode);

                    if (LocationUtils.isValidCountryCode(countryCode)) {
                        doc1.addField("countryCode", countryCode);
                        doc1.addField("continent", LocationUtils.getContinentCode(countryCode));
                    } else {
                        log.error("COUNTRY ERROR: " + countryCode);
                    }

                    doc1.addField("city", location.city);
                    doc1.addField("latitude", location.latitude);
                    doc1.addField("longitude", location.longitude);
                }
            }

            if(request.getHeader("User-Agent") != null)
            {
                doc1.addField("userAgent", request.getHeader("User-Agent"));
            }
        }


        doc1.addField("isBot",isSpiderBot);

        if(dspaceObject != null){
            doc1.addField("id", dspaceObject.getID());
            doc1.addField("type", dspaceObject.getType());
            storeParents(doc1, dspaceObject);
        }else{
            doc1.addField("type", Constants.SITE);
            //Art made me do it!
            doc1.addField("id", -1);
        }

        // Save the current time
        doc1.addField("time", DateFormatUtils.format(new Date(), DATE_FORMAT_8601));
        Calendar today = Calendar.getInstance();
        int month = (today.get(Calendar.MONTH) + 1);
        doc1.addField("dateYearMonth", today.get(Calendar.YEAR) + "-" + (month < 10 ? "0" + month : month));
        doc1.addField("dateYear", today.get(Calendar.YEAR));
        if (currentUser != null)
        {
            doc1.addField("epersonid", currentUser.getID());
        }
		for (SolrLoggerLogPlugin solrLoggerLogPlugin : new DSpace().getServiceManager().getServicesByType(SolrLoggerLogPlugin.class)) {
			solrLoggerLogPlugin.contributeToSolrDoc(dspaceObject, request, currentUser, doc1);
		}
        doc1.addField(CUAConstants.FIELD_CUA_VERSION, CUAConstants.MODULE_VERSION);
        return doc1;
    }

    public static void postSearch(DSpaceObject resultObject, HttpServletRequest request, EPerson currentUser,
                                 List<String> queries, List<String> filterQueries, int rpp, String sortBy, String order, int page, DSpaceObject scope) {
        try
        {
            SolrInputDocument solrDoc = getCommonSolrDoc(resultObject, request, currentUser);
            if (solrDoc == null) return;

            StringBuilder fullQuery = new StringBuilder();
            for (String query : queries) {
                solrDoc.addField("query", query);
                if(fullQuery.length()>0){
                    fullQuery.append(" AND ");
                }
                fullQuery.append(query);
            }
            solrDoc.addField("simple_query",fullQuery.toString());
            solrDoc.addField("simple_query_search",StringUtils.lowerCase(fullQuery.toString()));
            for (String filterquery : filterQueries) {
                solrDoc.addField("filterquery", filterquery);
                if(fullQuery.length()>0){
                    fullQuery.append(" AND ");
                }
                fullQuery.append(filterquery);
            }

            solrDoc.addField("complete_query", fullQuery.toString());
            solrDoc.addField("complete_query_search",StringUtils.lowerCase(fullQuery.toString()));


            if(resultObject != null){
                //We have a search result
                solrDoc.addField("statistics_type", StatisticsType.SEARCH_RESULT.text());
            }else{
                solrDoc.addField("statistics_type", StatisticsType.SEARCH.text());
            }
            //Store the scope
            if(scope != null){
                solrDoc.addField("scopeId", scope.getID());
                solrDoc.addField("scopeType", scope.getType());
            }

            if(rpp != -1){
                solrDoc.addField("rpp", rpp);
            }

            if(sortBy != null){
                solrDoc.addField("sortBy", sortBy);
                if(order != null){
                    solrDoc.addField("sortOrder", order);
                }
            }

            if(page != -1){
                solrDoc.addField("page", page);
            }

            solr.add(solrDoc);
        }
        catch (RuntimeException re)
        {
            throw re;
        }
        catch (Exception e)
        {
        	log.error(e.getMessage(), e);
        }
    }

    public static void postWorkflow(UsageWorkflowEvent usageWorkflowEvent) throws SQLException {
        try {
            SolrInputDocument solrDoc = getCommonSolrDoc(usageWorkflowEvent.getObject(), null, null);

            //Log the current collection & the scope !
            solrDoc.addField("owningColl", usageWorkflowEvent.getScope().getID());
            storeParents(solrDoc, usageWorkflowEvent.getScope());

            if(usageWorkflowEvent.getWorkflowStep() != null){
                solrDoc.addField("workflowStep", usageWorkflowEvent.getWorkflowStep());
            }
            if(usageWorkflowEvent.getOldState() != null){
                solrDoc.addField("previousWorkflowStep", usageWorkflowEvent.getOldState());
            }
            if(usageWorkflowEvent.getGroupOwners() != null){
                for (int i = 0; i < usageWorkflowEvent.getGroupOwners().length; i++) {
                    Group group = usageWorkflowEvent.getGroupOwners()[i];
                    solrDoc.addField("owner", "g" + group.getID());
                }
            }
            if(usageWorkflowEvent.getEpersonOwners() != null){
                for (int i = 0; i < usageWorkflowEvent.getEpersonOwners().length; i++) {
                    EPerson ePerson = usageWorkflowEvent.getEpersonOwners()[i];
                    solrDoc.addField("owner", "e" + ePerson.getID());
                }
            }

            solrDoc.addField("workflowItemId", usageWorkflowEvent.getWorkflowItem().getID());
			addMetadata(solrDoc, (Item) usageWorkflowEvent.getObject());

            EPerson submitter = ((Item) usageWorkflowEvent.getObject()).getSubmitter();
            if(submitter != null){
                solrDoc.addField("submitter", submitter.getID());
            }
            solrDoc.addField("statistics_type", StatisticsType.WORKFLOW.text());
            if(usageWorkflowEvent.getActor() != null){
                solrDoc.addField("actor", usageWorkflowEvent.getActor().getID());
                Group[] memberGroups = Group.allMemberGroups(usageWorkflowEvent.getContext(), usageWorkflowEvent.getActor());
                for (Group memberGroup : memberGroups) {
                    storeGroupAndParents(solrDoc, usageWorkflowEvent.getContext(), "actorMemberGroupId", memberGroup);
                }
            }

            /**
             * Log the groups which are currently executing this action !
             */
            Group[] actingGroups = usageWorkflowEvent.getActingGroups();
            if(actingGroups != null)
            {
                for (Group actingGroup : actingGroups) {
                    storeGroupAndParents(solrDoc, usageWorkflowEvent.getContext(), "actingGroupParentId", actingGroup);
                    storeGroupAndChildrenWhereMember(solrDoc, "actingGroupId", usageWorkflowEvent.getActor(), actingGroup);
                }
            }

            int bitstreamCount = 0;
            Item item = usageWorkflowEvent.getWorkflowItem().getItem();
            Bundle[] bundles = item.getBundles("ORIGINAL");
                for (Bundle bundle : bundles) {
                Bitstream[] bitstreams = bundle.getBitstreams();
                for (Bitstream bitstream : bitstreams) {
                    solrDoc.addField("bitstreamId", bitstream.getID());
                    bitstreamCount++;
                }
            }
            solrDoc.addField("bitstreamCount", bitstreamCount);

            SolrLogThread logThread = new SolrLogThread(solr, solrDoc);
            logThread.start();
            if (!MULTITHREADED)
                logThread.join();
        }
        catch (Exception e)
        {
            //Log the exception, no need to send it through, the workflow shouldn't crash because of this !
        	log.error(e.getMessage(), e);
        }

    }

    private static void storeGroupAndParents(SolrInputDocument solrInputDocument, Context context, String field, Group group) throws SQLException {
        List<Group> directParentGroups = CUAGroupUtils.getDirectParentGroups(context, group);
        for (Group parent : directParentGroups) {
            solrInputDocument.addField(field, parent.getID());
            storeGroupAndParents(solrInputDocument, context, field, parent);
        }

    }

    private static void storeGroupAndChildrenWhereMember(SolrInputDocument solrInputDocument, String field, EPerson eperson, Group group){
        if(isMember(group, eperson))
        {
            if(solrInputDocument.getFieldValues(field) == null || !solrInputDocument.getFieldValues(field).contains(group.getID())){
                solrInputDocument.addField(field, group.getID());
            }
            Group[] memberGroups = group.getMemberGroups();
            for (Group memberGroup : memberGroups) {
                storeGroupAndChildrenWhereMember(solrInputDocument, field, eperson, memberGroup);
            }
        }
    }

    /**
     * Recursive method that check if an eperson is a member of this group OR one of its subgroups
     * @param group
     * @param ePerson
     * @return
     */
    private static boolean isMember(Group group, EPerson ePerson)
    {
        if(group.isMember(ePerson))
        {
            return true;
        }else{
            Group[] memberGroups = group.getMemberGroups();
            for (Group memberGroup : memberGroups) {
                if (isMember(memberGroup, ePerson)) {
                    return true;
                }
            }
        }
        return false;

    }

	public static List<MetadataStorageInfo> getMetadataStorageInfoList() {
        return metadataStorageInfoList;
    }

    /**
     * Method just used to log the parents.
     * <ul>
     *  <li>Community log: owning comms.</li>
     *  <li>Collection log: owning comms & their comms.</li>
     *  <li>Item log: owning colls/comms.</li>
     *  <li>Bitstream log: owning item/colls/comms.</li>
     * </ul>
     * 
     * @param doc1
     *            the current SolrInputDocument
     * @param dso
     *            the current dspace object we want to log
     * @throws SQLException
     *             ignore it
     */
    public static void storeParents(SolrInputDocument doc1, DSpaceObject dso)
            throws SQLException
    {
        if (dso instanceof Community)
        {
            Community comm = (Community) dso;
            doc1.addField("containerCommunity", comm.getID());
            while (comm != null && comm.getParentCommunity() != null)
            {
                comm = comm.getParentCommunity();
                doc1.addField("owningComm", comm.getID());
                doc1.addField("containerCommunity", comm.getID());
            }
        }
        else if (dso instanceof Collection)
        {
            Collection coll = (Collection) dso;
            doc1.addField("containerCollection", coll.getID());
            for (int i = 0; i < coll.getCommunities().length; i++)
            {
                Community community = coll.getCommunities()[i];
                doc1.addField("owningComm", community.getID());
                storeParents(doc1, community);
            }
        }
        else if (dso instanceof Item)
        {
            Item item = (Item) dso;
            doc1.addField("containerItem", item.getID());
            for (int i = 0; i < item.getCollections().length; i++)
            {
                Collection collection = item.getCollections()[i];
                doc1.addField("owningColl", collection.getID());
                storeParents(doc1, collection);
            }
        }
        else if (dso instanceof Bitstream)
        {
            Bitstream bitstream = (Bitstream) dso;
            doc1.addField("containerBitstream", bitstream.getID());
            for (int i = 0; i < bitstream.getBundles().length; i++)
            {
                Bundle bundle = bitstream.getBundles()[i];
                for (int j = 0; j < bundle.getItems().length; j++)
                {
                    Item item = bundle.getItems()[j];
                    doc1.addField("owningItem", item.getID());
                    storeParents(doc1, item);
                }
            }
        }
    }

    public static boolean isUseProxies()
    {
        return useProxies;
    }

    /**
     * Delete data from the index, as described by a query.
     *
     * @param query description of the records to be deleted.
     * @throws IOException
     * @throws SolrServerException
     */
    public static void removeIndex(final String query)
    {
        visitEachStatisticShard(new ShardVisitor() {
            @Override
            public void visit(HttpSolrServer solr) {
                try {
        solr.deleteByQuery(query);
        count++;
        if (commit > 0 && count % commit == 0)
            solr.commit();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (SolrServerException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public static Map<String, List<String>> queryField(String query,
            List oldFieldVals, String field)
    {
        Map<String, List<String>> currentValsStored = new HashMap<String, List<String>>();
        try
        {
            // Get one document (since all the metadata for all the values
            // should be the same just get the first one we find
            Map<String, String> params = new HashMap<String, String>();
            params.put("q", query);
            params.put("rows", "1");
            MapSolrParams solrParams = new MapSolrParams(params);
            QueryResponse response = solr.query(solrParams);
            // Make sure we at least got a document
            if (response.getResults().getNumFound() == 0)
            {
                return currentValsStored;
            }

            // We have at least one document good
            SolrDocument document = response.getResults().get(0);
            for (MetadataStorageInfo metadataStorageInfo : metadataStorageInfoList)
            {
                // For each of these fields that are stored we are to create a
                // list of the values it holds now
                java.util.Collection collection = document
                        .getFieldValues((String) metadataStorageInfo.getSolrStatisticsField());
                List<String> storedVals = new ArrayList<String>();
                storedVals.addAll(collection);
                // Now add it to our hashmap
                currentValsStored.put((String) metadataStorageInfo.getSolrStatisticsField(), storedVals);
            }

            // System.out.println("HERE");
            // Get the info we need
        }
        catch (SolrServerException e)
        {
            e.printStackTrace();
        }
        return currentValsStored;
    }


    public static class ResultProcessor
    {

        public void execute(String query) throws SolrServerException, IOException {
			execute(query, null);
		}


		public void execute(String query, String field) throws SolrServerException, IOException {
            Map<String, String> params = new HashMap<String, String>();
            params.put("q", query);
            params.put("rows", "100");
			if (StringUtils.isNotBlank(field)) {
				params.put("fl", field);
			}

            if(1 < statisticYearCores.size()){
                params.put(ShardParams.SHARDS, StringUtils.join(statisticYearCores.iterator(), ','));
            }
            MapSolrParams solrParams = new MapSolrParams(params);
            QueryResponse response = solr.query(solrParams);

            long numbFound = response.getResults().getNumFound();

            // process the first batch
            process(response.getResults());

            // Run over the rest
            for (int i = 100; i < numbFound; i += 100)
            {
                params.put("start", String.valueOf(i));
                solrParams = new MapSolrParams(params);
                response = solr.query(solrParams);
                process(response.getResults());
            }

        }

        public void commit() {
            visitEachStatisticShard(new ShardVisitor() {
                @Override
                public void visit(HttpSolrServer solr) {
                    try {
            solr.commit();
                    } catch (SolrServerException | IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        /**
         * Override to manage pages of documents
         * @param docs
         */
        public void process(List<SolrDocument> docs) throws IOException, SolrServerException {
            for(SolrDocument doc : docs){
                process(doc);
            }
        }

        /**
         * Override to manage individual documents
         * @param doc
         */
        public void process(SolrDocument doc) throws IOException, SolrServerException {


        }
    }


	public static void markRobotsByIP() {
		for (final String ip : SpiderDetector.getSpiderIpAddresses()) {

            visitEachStatisticShard(new ShardVisitor() {
                @Override
                public void visit(final HttpSolrServer solr) {
            try {

                /* Result Process to alter record to be identified as a bot */
                ResultProcessor processor = new ResultProcessor(){
                    public void process(SolrDocument doc) throws IOException, SolrServerException {
                        doc.removeFields("isBot");
                        doc.addField("isBot", true);
                        SolrInputDocument newInput = ClientUtils.toSolrInputDocument(doc);
                        solr.add(newInput);
                        log.info("Marked " + doc.getFieldValue("ip") + " as bot");
                    }
                };

                /* query for ip, exclude results previously set as bots. */
                processor.execute("ip:"+ip+ "* AND -isBot:true");

                solr.commit();

            } catch (Exception e) {
                log.error(e.getMessage(),e);
            }


        }
            });

    }
    }

    public static void markRobotByUserAgent(final String agent) {
        visitEachStatisticShard(new ShardVisitor() {
            @Override
            public void visit(final HttpSolrServer solr) {

        try {

                /* Result Process to alter record to be identified as a bot */
                ResultProcessor processor = new ResultProcessor(){
                    public void process(SolrDocument doc) throws IOException, SolrServerException {
                        doc.removeFields("isBot");
                        doc.addField("isBot", true);
                        SolrInputDocument newInput = ClientUtils.toSolrInputDocument(doc);
                        solr.add(newInput);
                    }
                };

                /* query for ip, exclude results previously set as bots. */
                processor.execute("userAgent:"+agent+ " AND -isBot:true");

                solr.commit();
            } catch (Exception e) {
                log.error(e.getMessage(),e);
            }
    }
        });
    }

    public static void markRobotByUserAgent() {
        visitEachStatisticShard(new ShardVisitor() {
            @Override
            public void visit(final HttpSolrServer solr) {
        try {

            final List<String> userAgentToUpdate = new ArrayList<String>();
                    /* Result Process to alter record to be identified as a bot */
            ResultProcessor processor = new ResultProcessor() {
                public void process(SolrDocument doc) throws IOException, SolrServerException {
                    String ip = (String) doc.getFieldValue("ip");
                    String userAgent = (String) doc.getFieldValue("userAgent");
                    if (StringUtils.isNotBlank(userAgent) && (userAgentToUpdate.contains(userAgent) || SpiderDetector.isSpider(ip, null, null, userAgent))) {
                        if (!userAgentToUpdate.contains(userAgent)) {
                            log.info("marked user agent " + userAgent + " as bot.");
                            userAgentToUpdate.add(userAgent);
                        }
                        doc.removeFields("isBot");
                        doc.addField("isBot", true);
                        SolrInputDocument newInput = ClientUtils.toSolrInputDocument(doc);
                        solr.add(newInput);
                    }
                }
            };
            /* query for ip, exclude results previously set as bots. */
            processor.execute("userAgent:[* TO *] AND -isBot:true");

            if (userAgentToUpdate.size() > 0) {
                StringBuilder query = new StringBuilder();
                Iterator agents = userAgentToUpdate.iterator();
                query.append("(");
                while (agents.hasNext()) {
                    query.append("userAgent:\"" + agents.next() + "\"");
                    if (agents.hasNext())
                        query.append(" OR ");
                }
                query.append(")");
                solr.deleteByQuery(query.toString() + " AND -isBot:true");
            }

            solr.commit();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
        });
    }


    public static void deleteRobotsByIsBotFlag() {
        visitEachStatisticShard(new ShardVisitor() {
            @Override
            public void visit(HttpSolrServer solr) {
        try {
           solr.deleteByQuery("isBot:true");
        } catch (Exception e) {
           log.error(e.getMessage(),e);
        }
    }
        });
    }

    public static void deleteIP(final String ip)
    {
        visitEachStatisticShard(new ShardVisitor() {
            @Override
            public void visit(HttpSolrServer solr) {
        try {
            solr.deleteByQuery("ip:"+ip + "*");
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }
        });
    }


	public static void deleteRobotsByIP() {
		for (String ip : SpiderDetector.getSpiderIpAddresses()) {
            deleteIP(ip);
        }
    }

    public static void removeByDSpaceObject(int id, int type, boolean commit)
    {
        String containerString;
        switch (type)
        {
            case Constants.COMMUNITY:
                containerString = "containerCommunity";
                break;
            case Constants.COLLECTION:
                containerString = "containerCollection";
                break;
            case Constants.ITEM:
                containerString = "containerItem";
                break;
            case Constants.BITSTREAM:
                containerString = "containerBitstream";
                break;
            default:
                return;
        }
        removeByQuery(containerString + ":"  + id, commit);
    }


    public static void removeByQuery(final String query, final boolean commit)
    {

        visitEachStatisticShard(new ShardVisitor() {
            @Override
            public void visit(HttpSolrServer solr) {
        try {
            solr.deleteByQuery(query);
            if(commit){
                solr.commit();
            }
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }
        });
    }

    /*
     * update(String query, boolean addField, String fieldName, Object
     * fieldValue, Object oldFieldValue) throws SolrServerException, IOException
     * { List<Object> vals = new ArrayList<Object>(); vals.add(fieldValue);
     * List<Object> oldvals = new ArrayList<Object>(); oldvals.add(fieldValue);
     * update(query, addField, fieldName, vals, oldvals); }
     * Deprecated in CUA. Fallback method.
     */
    @Deprecated
    public static void update(String query, String action,
            List<String> fieldNames, List<List<Object>> fieldValuesList)
            throws SolrServerException, IOException
    {

        List<UpdateComponent> updateComponents = new ArrayList<>();

        for (int j = 0; j < fieldNames.size(); j++) {
            String fieldName = fieldNames.get(j);
            List<Object> fieldValues = fieldValuesList.get(j);

            if (action.equals("addOne")) {
                if (fieldValues.size() > 0) {
                    AddValues addOne = new AddValues(fieldName);
                    for (Object fieldValue : fieldValues) {
                        addOne.addValue("" + fieldValue);
                    }
                    updateComponents.add(addOne);
                }
            } else if (action.equals("replace")) {
                if (fieldValues.size() > 0) {
                    ReplaceValues replace = new ReplaceValues(fieldName);
                    for (Object fieldValue : fieldValues) {
                        replace.addValue("" + fieldValue);
                    }
                    updateComponents.add(replace);
                }
            } else if (action.equals("remOne")) {

                RemoveValues remOne = new RemoveValues(fieldName);
                for (Object fieldValue : fieldValues) {
                    remOne.addValue("" + fieldValue);
                }
                updateComponents.add(remOne);


            } else if (action.equals("addOneNotPresent")) {
                AddValuesIfNotPresent addOneNotPresent = new AddValuesIfNotPresent(fieldName);
                for (Object fieldValue : fieldValues) {
                    addOneNotPresent.addValue("" + fieldValue);
                }
                updateComponents.add(addOneNotPresent);


            } else {
                throw new UnsupportedOperationException("This action is not supported :"+action);

            }

        }

        update(query, updateComponents, true, -1, 100);
    }


    public static void update(final String query, final List<UpdateComponent> updateComponents, final boolean commit, final int sleepMs, final int rows) {
        visitEachStatisticShard(new ShardVisitor() {
            @Override
            public void visit(HttpSolrServer solr) {
        try {
                    String query1 = query;

            Set<String> targetFieldsHash = new HashSet<>();
            Set<String> requiredFields = new HashSet<>();
            requiredFields.add("uid");
            requiredFields.add("time");
            requiredFields.add("statistics_type");

            for (UpdateComponent updateComponent : updateComponents) {
                String[] targetFields = updateComponent.getTargetFields();
                for (String targetField : targetFields) {
                    if (targetFieldsHash.contains(targetField)) {
                        throw new UnsupportedOperationException("Multiple update components target the same field:" + targetField);
                    }
                    targetFieldsHash.add(targetField);
                }
                requiredFields.addAll(Arrays.asList(updateComponent.getRequiredFields()));
            }

                    String info = "Query:" + query1;
            System.out.println(Calendar.getInstance().getTime() + " | " + info);
            log.info(info);

            Date date = new Date();
            solr.commit(false,false);
            if(MULTITHREADED){
                        query1 = query1 + " AND -time:[" + DateFormatUtils.format(date, DATE_FORMAT_8601) + " TO *]";
            }

            Map<String, String> params = new HashMap<>();
                    params.put("q", query1);
            params.put("rows", "" + rows);
            params.put("sort", "time desc");
            params.put("fl", StringUtils.join(requiredFields, ","));

//            if (1 < statisticYearCores.size()) {
//                params.put(ShardParams.SHARDS, StringUtils.join(statisticYearCores.iterator(), ','));
//            }

            boolean hasMore = true;
            for (int k = 0; hasMore; k += rows) {

                params.put("start", String.valueOf(k));
                MapSolrParams solrParams = new MapSolrParams(params);
                QueryResponse response = solr.query(solrParams);
                long numbFound = response.getResults().getNumFound();
                List<SolrDocument> docsToUpdate = response.getResults();
                ArrayList<SolrInputDocument> inputDocuments = new ArrayList<>();

                if (docsToUpdate.size() == 0) {
                    hasMore = false;
                } else {
                    // Add the new updated onces
                    info = "Updating : " + (k + response.getResults().size()) + "/" + numbFound + " docs in " + solr.getBaseURL();
                    System.out.println(Calendar.getInstance().getTime() + " | " + info);
                    log.info(info);
                    for (int i = 0; i < docsToUpdate.size(); i++) {

                        SolrDocument solrDocument = docsToUpdate.get(i);

                        SolrInputDocument newInput = new SolrInputDocument();
                        for (String requiredField : requiredFields) {
                            newInput.addField(requiredField, solrDocument.getFieldValue(requiredField));
                        }

                        for (UpdateComponent updateComponent : updateComponents) {
                            updateComponent.execute(solrDocument, newInput);
                        }

                        inputDocuments.add(newInput);
                    }

                    if (inputDocuments.size() > 0) {
                        solr.add(inputDocuments);
                    }

                }

                if (hasMore && (sleepMs > 0)) {
                    try {
                        Thread.sleep(sleepMs);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }

            }
            count++;
            if (commit) {
                System.out.println("Commit");
                solr.commit();
                System.out.println("Commit done");
            }
            // System.out.println("SolrLogger.update(\""+query+"\"):"+(new
            // Date().getTime() - start)+"ms,"+numbFound+"records");
        } catch (Exception e) {
            System.out.println("Error while updating");
            e.printStackTrace();
                    try {
            solr.rollback();
                    } catch (SolrServerException | IOException e1) {
                        throw new RuntimeException(e1);
                    }
            throw new RuntimeException(e);
        }

            }
        });
    }


    public static void addDocument(SolrDocument solrDocument, boolean commit) throws IOException, SolrServerException {
        if(solrDocument != null){
            SolrInputDocument newInput = ClientUtils.toSolrInputDocument(solrDocument);
            solr.add(newInput);
        }
        if(commit)
            solr.commit();

    }

    public static void query(String query, int max) throws SolrServerException
    {
        query(query, Collections.<String>emptyList(), null, true, max, null, null, null, null);

    }

    /**
     * Query used to get values grouped by the given facetfield
     *
     * @param query
     *            the query to be used
     * @param facetField
     *            the facet field on which to group our values
     * @param max
     *            the max number of values given back (in case of 10 the top 10
     *            will be given)
     * @param showTotal
     *            a boolean determening whether the total amount should be given
     *            back as the last element of the array
     * @return an array containing our results
     * @throws SolrServerException
     *             ...
     */
    public static ObjectCount[] queryFacetField(String query,
                                                String filterQuery, String facetField, int max, boolean showTotal,
												List<String> facetQueries) throws SolrServerException {
		return queryFacetField(query, Collections.singletonList(filterQuery), facetField, max, showTotal, facetQueries);
	}

	/**
	 * Query used to get values grouped by the given facetfield
	 *
	 * @param query       the query to be used
	 * @param filterQuery
	 * @param facetField  the facet field on which to group our values
	 * @param max         the max number of values given back (in case of 10 the top 10
	 *                    will be given)
	 * @param showTotal   a boolean determening whether the total amount should be given
	 *                    back as the last element of the array    @return an array containing our results
	 * @throws SolrServerException ...
	 */
	public static ObjectCount[] queryFacetField(String query,
												List<String> filterQuery, String facetField, int max, boolean showTotal,
												List<String> facetQueries) throws SolrServerException {
        return queryFacetField(query,filterQuery,facetField, true ,max,0, showTotal,facetQueries);
    }

    /**
     * Query used to get values grouped by the given facetfield
     *
     * @param query
     *            the query to be used
     * @param facetField
     *            the facet field on which to group our values
     * @param max
     *            the max number of values given back (in case of 10 the top 10
     *            will be given)
     * @param showTotal
     *            a boolean determening whether the total amount should be given
     *            back as the last element of the array
     * @return an array containing our results
     * @throws SolrServerException
     *             ...
     */
    public static ObjectCount[] queryFacetField(String query,
                                                String filterQuery, String facetField, boolean sort, int max, int offset, boolean showTotal,
                                                List<String> facetQueries) throws SolrServerException {
        return queryFacetField(query, Collections.singletonList(filterQuery), facetField, sort, max, offset, showTotal, facetQueries);
    }

    /**
     * Query used to get values grouped by the given facetfield
     * 
     * @param query
     *            the query to be used
     * @param filterQuery
     *@param facetField
     *            the facet field on which to group our values
     * @param max
 *            the max number of values given back (in case of 10 the top 10
 *            will be given)
     * @param showTotal
*            a boolean determening whether the total amount should be given
*            back as the last element of the array    @return an array containing our results
     * @throws SolrServerException
     *             ...
     */
    public static ObjectCount[] queryFacetField(String query,
            List<String> filterQuery, String facetField,boolean sort, int max, int offset, boolean showTotal,
            List<String> facetQueries) throws SolrServerException
    {
        List<String> facetFields = new ArrayList<String>();
        facetFields.add(facetField);
		return queryFacetField(new SolrLoggerParametersBuilder().withRows(0).withQuery(query).withFilterQuery(filterQuery).withFacetFields(facetFields).withFacetSort(sort).withMax(max).withFacetOffset(offset).withFacetQueries(facetQueries).create(), showTotal);
	}

	public static ObjectCount[] queryFacetField(SolrLoggerParams params) throws SolrServerException {
		return queryFacetField(new SolrLoggerParametersBuilder().create(params), params.isShowTotal());
	}


	public static ObjectCount[] queryFacetField(SolrLoggerParameters parameters, boolean showTotal) throws SolrServerException {

		QueryResponse queryResponse = query(parameters);
		if (queryResponse == null) {
            return new ObjectCount[0];
        }

		FacetField field = queryResponse.getFacetField(parameters.getFacetFields().get(0));
        // At least make sure we have one value
        if (0 < field.getValueCount())
        {
            // Create an array for our result
            ObjectCount[] result = new ObjectCount[field.getValueCount()
                    + (showTotal ? 1 : 0)];
            // Run over our results & store them
            for (int i = 0; i < field.getValues().size(); i++)
            {
                FacetField.Count fieldCount = field.getValues().get(i);
                result[i] = new ObjectCount();
                result[i].setCount(fieldCount.getCount());
                result[i].setValue(fieldCount.getName());
            }
            if (showTotal)
            {
                result[result.length - 1] = new ObjectCount();
                result[result.length - 1].setCount(queryResponse.getResults()
                        .getNumFound());
                result[result.length - 1].setValue("total");
            }
            return result;
        }
        else
        {
            // Return an empty array cause we got no data
            if(showTotal){
                ObjectCount objectCount = new ObjectCount();
                objectCount.setCount(0L);
                objectCount.setValue("total");
                return new ObjectCount[]{objectCount};
            }
            return new ObjectCount[0];
        }
    }

    /**
     * Query used to get values grouped by the given facetfield
     * @param query the query to be used
     * @param facetFields the facet fields on which to group our values
     * @param max the max number of values given back (in case of 10 the top 10 will be given)
     * @param showTotal a boolean determening whether the total amount should be given back as the last element of the array
     * @return a map with as a key the facetfield and as a value the results
     * @throws SolrServerException ...
     */
    public static Map<String, ObjectCount[]> queryMultipleFacetFields(String query, String filterQuery, List<String> facetFields, int max, String dateType, String dateStart, String dateEnd, boolean showTotal, List<String> facetQueries) throws SolrServerException {
        QueryResponse queryResponse = query(query, filterQuery, facetFields, true, max, dateType, dateStart, dateEnd, facetQueries);
        if(queryResponse == null)
            return new HashMap<String, ObjectCount[]>();

        Map<String, ObjectCount[]> resultMap = new HashMap<String, ObjectCount[]>();
        for (String facetField : facetFields) {
            FacetField field = queryResponse.getFacetField(facetField);
            //At least make sure we have one value
            if (0 < field.getValueCount()) {
                //Create an array for our result
                ObjectCount[] result = new ObjectCount[field.getValueCount() + (showTotal ? 1 : 0)];
                //Run over our results & store them
                for (int j = 0; j < field.getValues().size(); j++) {
                    FacetField.Count fieldCount = field.getValues().get(j);
                    result[j] = new ObjectCount();
                    result[j].setCount(fieldCount.getCount());
                    result[j].setValue(fieldCount.getName());
                }
                if (showTotal) {
                    result[result.length - 1] = new ObjectCount();
                    result[result.length - 1].setCount(queryResponse.getResults().getNumFound());
                    result[result.length - 1].setValue("total");
                }
                resultMap.put(facetField, result);
            } else {
                //Return an empty array cause we got no data
                resultMap.put(facetField, new ObjectCount[0]);
            }

        }
        return resultMap;
    }

    /**
     * Query used to get values grouped by the date.
     *
     * @param query
     *            the query to be used
     * @param max
     *            the max number of values given back (in case of 10 the top 10
     *            will be given)
     * @param dateType
     *            the type to be used (example: DAY, MONTH, YEAR)
     * @param dateStart
     *            the start date Format:(-3, -2, ..) the date is calculated
     *            relatively on today
     * @param dateEnd
     *            the end date stop Format (-2, +1, ..) the date is calculated
     *            relatively on today
     * @param showTotal
     *            a boolean determining whether the total amount should be given
     *            back as the last element of the array
     * @return and array containing our results
     * @throws SolrServerException
     *             ...
     */
    public static ObjectCount[] queryFacetDate(String query,
                                               String filterQuery, List<String> facetQueries, int max, String dateType, String dateStart,
                                               String dateEnd, boolean showTotal) throws SolrServerException, ParseException {
        return queryFacetDate(query, Collections.singletonList(filterQuery), facetQueries, max, dateType, dateStart, dateEnd, showTotal);
    }

    /**
     * Query used to get values grouped by the date.
     *
     * @param query
     *            the query to be used
     * @param max
     *            the max number of values given back (in case of 10 the top 10
     *            will be given)
     * @param dateType
     *            the type to be used (example: DAY, MONTH, YEAR)
     * @param dateStart
     *            the start date Format:(-3, -2, ..) the date is calculated
     *            relatively on today
     * @param dateEnd
     *            the end date stop Format (-2, +1, ..) the date is calculated
     *            relatively on today
     * @param showTotal
     *            a boolean determining whether the total amount should be given
     *            back as the last element of the array
     * @return and array containing our results
     * @throws SolrServerException
     *             ...
     */
    public static ObjectCount[] queryFacetDate(String query,
                                               List<String> filterQuery, int max, String dateType, String dateStart,
                                               String dateEnd, boolean showTotal) throws SolrServerException, ParseException {
        return queryFacetDate(query, filterQuery, new ArrayList<String>(), max, dateType, dateStart, dateEnd, showTotal);
    }

	/**
	 * Query used to get values grouped by the date.
	 *
	 * @param query       the query to be used
	 * @param filterQuery
	 * @param max         the max number of values given back (in case of 10 the top 10
	 *                    will be given)
	 * @param dateType    the type to be used (example: DAY, MONTH, YEAR)
	 * @param dateStart   the start date Format:(-3, -2, ..) the date is calculated
	 *                    relatively on today
	 * @param dateEnd     the end date stop Format (-2, +1, ..) the date is calculated
	 *                    relatively on today
	 * @param showTotal   a boolean determining whether the total amount should be given
	 *                    back as the last element of the array      @return and array containing our results
	 * @throws SolrServerException ...
	 */
	public static ObjectCount[] queryFacetDate(String query,
                                               List<String> filterQuery, List<String> facetQueries, int max, String dateType, String dateStart,
											   String dateEnd, boolean showTotal) throws SolrServerException, ParseException {
        //Resolve our type
        String facet = null;
        if(ConfigurationManager.getBooleanProperty("atmire-cua", "statistics.efficientDatefaceting", true)){
            if("month".equalsIgnoreCase(dateType))
                facet = "dateYearMonth";
            else
            if("year".equalsIgnoreCase(dateType)){
                facet = "dateYear";
            }
        }

        QueryResponse queryResponse;
        ObjectCount[] result;
        if(facet == null){
            //Should we be faceting on something else then month or year (like a day for example)
            facet = "time";
            queryResponse = query(query, filterQuery, null, true, max,
                    dateType, dateStart, dateEnd, facetQueries);

            if (queryResponse == null)
                return new ObjectCount[0];

            FacetField dateFacet = queryResponse.getFacetDate(facet);

            if(dateFacet.getValues() == null)
                return new ObjectCount[0];


            // Create an array for our result
            result = new ObjectCount[dateFacet.getValueCount()
                    + (showTotal ? 1 : 0)];
            // Run over our datefacet & store all the values
            for (int i = 0; i < dateFacet.getValues().size(); i++)
            {
                FacetField.Count dateCount = dateFacet.getValues().get(i);
                result[i] = new ObjectCount();
                result[i].setCount(dateCount.getCount());
                result[i].setValue(getDateView(dateCount.getName(), dateType));
            }
            //TODO: test this !
            if (showTotal)
            {
                ObjectCount count = new ObjectCount();
                count.setCount(0);
                // TODO: Make sure that this total is gotten out of the msgs.xml
                count.setValue("total");
//                result[result.length - 1] = (count.getValue(), count);
            }
        }else{
            //Parse our dates
            DateMathParser parser = new DateMathParser(Calendar.getInstance().getTimeZone(), Locale.getDefault());
            Calendar startDate = Calendar.getInstance();
            if("0".equals(dateStart)) {
                startDate.setTime(parser.parseMath("+"+dateStart + dateType + "S"));
            } else{

                startDate.setTime(parser.parseMath(dateStart + dateType + "S"));
            }
            Calendar endDate = Calendar.getInstance();
            //Make sure we aren't requesting now
            if(!"0".equals(dateEnd)) {
                endDate.setTime(parser.parseMath(dateEnd + dateType));
            }

            int spread = endDate.get(Calendar.YEAR) - startDate.get(Calendar.YEAR);
            //Add one to our spread (if we should be requesting data from one year/month only)
            spread++;
            if("month".equalsIgnoreCase(dateType)){
                //Since we need months, multiply by 12
                spread *= 12;
            }

            //We need to put all possible values in a map with as key the value, one empty objectcount by default these will be filled in later
            LinkedHashMap<String, ObjectCount> valuesIndexList = new LinkedHashMap<String, ObjectCount>();
            int calendarType = "month".equalsIgnoreCase(dateType) ? Calendar.MONTH : Calendar.YEAR;
            DateFormat dateFormat = new SimpleDateFormat(calendarType == Calendar.MONTH ? "yyyy-MM" : "yyyy");

            //Add one empty objectcount object for each month/year
            while(startDate.get(Calendar.YEAR) != endDate.get(Calendar.YEAR) || (calendarType == Calendar.MONTH && startDate.get(Calendar.MONTH) != endDate.get(Calendar.MONTH))){
                //Store the current type
                String formattedDate = dateFormat.format(startDate.getTime());
                ObjectCount count = new ObjectCount();
                count.setCount(0);
                count.setValue(formattedDate);
                valuesIndexList.put(formattedDate, count);
                //Add one so we move along
                startDate.add(calendarType, 1);
            }
            if (showTotal)
            {
                ObjectCount count = new ObjectCount();
                count.setCount(0);
                // TODO: Make sure that this total is gotten out of the msgs.xml
                count.setValue("total");
                valuesIndexList.put(count.getValue(), count);
            }


            queryResponse = query(query, filterQuery, Arrays.asList(facet), false, spread,
                    null, null, null, facetQueries);

            if (queryResponse != null && queryResponse.getFacetField(facet).getValues() != null){
                FacetField dateFacet = queryResponse.getFacetField(facet);
                // Loop over our results from our query
                for (int i = 0; i < dateFacet.getValues().size(); i++) {
                    FacetField.Count dateCount = dateFacet.getValues().get(i);
                    //Retrieve our 0 count objectcount & fill the result
                    ObjectCount objectCount = valuesIndexList.get(dateCount.getName());
                    if(objectCount == null)
                        continue;
                    objectCount.setCount(dateCount.getCount());
                    //Put it back
                    valuesIndexList.put(objectCount.getValue(), objectCount);
                }
            }
            if(showTotal){
                ObjectCount objectCount = valuesIndexList.get("total");
                objectCount.setCount(queryResponse != null ? queryResponse.getResults().getNumFound() : 0);
                valuesIndexList.put("total", objectCount);
            }

            //Put our values in our resulting array
            result = valuesIndexList.values().toArray(new ObjectCount[valuesIndexList.size()]);
        }

        return result;
    }

    public static ObjectCount[] queryFacetQuery(SolrLoggerParams params) throws SolrServerException {
        return queryFacetQuery(params, true);
    }

    public static ObjectCount[] queryFacetQuery(SolrLoggerParams params, boolean sort) throws SolrServerException {
        int max = params.getMax();
        params.setMax(Integer.MAX_VALUE);
        Map<String, Integer> results = queryFacetQuery(params.getQuery(), params.getFilterQuery(), params.getFacetQueries());
        List<ObjectCount> objectCounts = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : results.entrySet()) if (entry.getValue() > 0) {
            ObjectCount objectCount = new ObjectCount();
            objectCount.setCount(entry.getValue());
            objectCount.setValue(entry.getKey());
            objectCounts.add(objectCount);
        }

        if (sort) Collections.sort(objectCounts, new Comparator<ObjectCount>() {
            @Override
            public int compare(ObjectCount o1, ObjectCount o2) {
                return Math.round(o2.getCount() - o1.getCount());
            }
        });

        if (max < objectCounts.size())
            objectCounts = objectCounts.subList(0, max);

        return objectCounts.toArray(new ObjectCount[objectCounts.size()]);
    }

    public static Map<String, Integer> queryFacetQuery(String query,
                                                       List<String> filterQueries, List<String> facetQueries)
            throws SolrServerException
    {
        filterQueries.add(filterQuery);
        QueryResponse response = query(query, filterQueries, null, true,1, null, null,
                null, facetQueries);
        return response.getFacetQuery();
    }

    public static ObjectCount queryTotal(String query, String filterQuery)
            throws SolrServerException
    {
        QueryResponse queryResponse = query(query, filterQuery, null, true,-1, null,
                null, null, null);
        ObjectCount objCount = new ObjectCount();
        if(queryResponse == null)
            return objCount;

        objCount.setCount(queryResponse.getResults().getNumFound());

        return objCount;
    }

    private static String getDateView(String name, String type)
    {
        if (name != null && name.matches("^[0-9]{4}\\-[0-9]{2}.*"))
        {
            /*
             * if("YEAR".equalsIgnoreCase(type)) return name.substring(0, 4);
             * else if("MONTH".equalsIgnoreCase(type)) return name.substring(0,
             * 7); else if("DAY".equalsIgnoreCase(type)) return
             * name.substring(0, 10); else if("HOUR".equalsIgnoreCase(type))
             * return name.substring(11, 13);
             */
            // Get our date
            Date date = null;
            try
            {
                SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT_8601);
                date = format.parse(name);
            }
            catch (ParseException e)
            {
                try
                {
                    // We should use the dcdate (the dcdate is used when
                    // generating random data)
                    SimpleDateFormat format = new SimpleDateFormat(
                            DATE_FORMAT_DCDATE);
                    date = format.parse(name);
                }
                catch (ParseException e1)
                {
                    e1.printStackTrace();
                }
                // e.printStackTrace();
            }
            String dateformatString = "dd-MM-yyyy";
            if ("DAY".equals(type))
            {
                dateformatString = "dd-MM-yyyy";
            }
            else if ("MONTH".equals(type))
            {
                dateformatString = "MMMM yyyy";

            }
            else if ("YEAR".equals(type))
            {
                dateformatString = "yyyy";
            }
            SimpleDateFormat simpleFormat = new SimpleDateFormat(
                    dateformatString);
            if (date != null)
            {
                name = simpleFormat.format(date);
            }

        }
        return name;
    }


    public static QueryResponse query(String query, String filterQuery, List<String> facetFields, boolean facetSort,
                                      int max, String dateType, String dateStart, String dateEnd, List<String>facetQueries) throws SolrServerException {
        return query(query, filterQuery, facetFields, facetSort, max, 0, dateType, dateStart, dateEnd, facetQueries, 0, 0, null, false,null);
    }

	public static QueryResponse query(String query, List<String> filterQuery, List<String> facetFields, boolean facetSort,
									  int max, String dateType, String dateStart, String dateEnd, List<String> facetQueries) throws SolrServerException {
		return query(filterQuery, query, facetFields, facetSort, max, 0, dateType, dateStart, dateEnd, facetQueries, 0, 0, null, false, null);
	}

      public static QueryResponse query(String query, String filterQuery, List<String> facetFields, boolean facetSort,
                                        int max, int facetOffset, String dateType, String dateStart, String dateEnd, List<String> facetQueries, int rows, int start, String sortField, boolean sortDesc) throws SolrServerException {
          return query(Collections.singletonList(filterQuery), query,  facetFields, facetSort, max, facetOffset, dateType, dateStart, dateEnd, facetQueries, rows, start, sortField, sortDesc);
      }

    public static QueryResponse query(List<String> filterQuery, String query,  List<String> facetFields, boolean facetSort,
                                      int max, int facetOffset, String dateType, String dateStart, String dateEnd, List<String>facetQueries, int rows, int start, String sortField, boolean sortDesc) throws SolrServerException {
        return query( filterQuery, query, facetFields, facetSort, max, facetOffset, dateType, dateStart, dateEnd, facetQueries, rows, start, sortField, sortDesc ,null);
    }


    public static QueryResponse query(String query, String filterQuery, List<String> facetFields, boolean facetSort,
                                      int max, int facetOffset, String dateType, String dateStart, String dateEnd, List<String> facetQueries, int rows, int start, String sortField, boolean sortDesc, List<String> statFields) throws SolrServerException {
        return query(Collections.singletonList(filterQuery), query, facetFields, facetSort, max, facetOffset, dateType, dateStart, dateEnd, facetQueries, rows, start, sortField, sortDesc, statFields);
    }

    public static QueryResponse query(List<String> filterQuery,String query, List<String> facetFields, boolean facetSort,
                                      int max, int facetOffset, String dateType, String dateStart, String dateEnd, List<String>facetQueries, int rows, int start, String sortField, boolean sortDesc,List<String> statFields) throws SolrServerException {

        boolean applyDefaultInternalFilter = true;
        if (filterQuery != null) {
            for (String _filterQuery : filterQuery) {
                if (_filterQuery != null && _filterQuery.contains("isInternal:")) {
                    applyDefaultInternalFilter = false;
                }
            }
        }

        if (applyDefaultInternalFilter && query != null && query.contains("isInternal:")) {
            applyDefaultInternalFilter = false;
        }

		boolean applyDefaultBotFilter = true;
		if (filterQuery != null) {
			for (String _filterQuery : filterQuery) {
				if (_filterQuery != null && _filterQuery.contains("isBot:")) {
					applyDefaultBotFilter = false;
				}
			}
		}

		if (applyDefaultBotFilter && query != null && query.contains("isBot:")) {
			applyDefaultBotFilter = false;
		}

		return query(new SolrLoggerParameters(filterQuery, query, facetFields, facetSort, max, facetOffset, dateType, dateStart, dateEnd, facetQueries, rows, start, sortField, sortDesc, statFields, applyDefaultInternalFilter, applyDefaultBotFilter, true, SolrRequest.METHOD.POST, 1, null, SolrLoggerParametersBuilder.defaultEnumMinDf));
	}

	public static QueryResponse query(List<String> filterQuery, String query, List<String> facetFields, boolean facetSort,
									  int max, int facetOffset, String dateType, String dateStart, String dateEnd, List<String> facetQueries, int rows, int start, String sortField, boolean sortDesc, List<String> statFields, boolean applyDefaultInternalFilter, boolean applyDefaultBotFilter, boolean applyBundleFilter, SolrRequest.METHOD method) throws SolrServerException {
		return query(new SolrLoggerParameters(filterQuery, query, facetFields, facetSort, max, facetOffset, dateType, dateStart, dateEnd, facetQueries, rows, start, sortField, sortDesc, statFields, applyDefaultInternalFilter, applyDefaultBotFilter, applyBundleFilter, method, 1, null, SolrLoggerParametersBuilder.defaultEnumMinDf));
	}

	public static QueryResponse query(SolrLoggerParams p) throws SolrServerException {
		return query(new SolrLoggerParametersBuilder().create(p));
	}


	public static QueryResponse query(SolrLoggerParameters solrLoggerParameters) throws SolrServerException {
		if (solr == null) {
            return null;
        }

        // System.out.println("QUERY");
		SolrQuery solrQuery = new SolrQuery().setRows(solrLoggerParameters.getRows() == -1 ? 0 : solrLoggerParameters.getRows()).setQuery(solrLoggerParameters.getQuery());

        addAdditionalSolrYearCores(solrQuery);
        // Set the date facet if present



		if (solrLoggerParameters.getDateType() != null) {
            solrQuery.setParam("facet.date", "time")
                    .
                    // EXAMPLE: NOW/MONTH+1MONTH
									setParam("facet.date.end", ("0".equals(solrLoggerParameters.getDateEnd()) ? "NOW/" + solrLoggerParameters.getDateType() : "NOW/" + solrLoggerParameters.getDateType() + solrLoggerParameters.getDateEnd() + solrLoggerParameters.getDateType())).
					setParam("facet.date.gap", "+1" + solrLoggerParameters.getDateType()).
                    // EXAMPLE: NOW/MONTH-" + nbMonths + "MONTHS
							setParam("facet.date.start", ("0".equals(solrLoggerParameters.getDateStart()) ? "NOW/" + solrLoggerParameters.getDateType() : "NOW/" + solrLoggerParameters.getDateType() + solrLoggerParameters.getDateStart() + solrLoggerParameters.getDateType() + "S")).
                    setFacet(true).setFacetMinCount(0);
        }
		if (solrLoggerParameters.getStatFields() != null && solrLoggerParameters.getStatFields().size() > 0) {
            solrQuery.setParam("stats", "true");
			solrQuery.setParam("stats.field", solrLoggerParameters.getStatFields().toArray(new String[solrLoggerParameters.getStatFields().size()]));

        }
		if (solrLoggerParameters.getStart() > 0) {
			solrQuery.setParam(CommonParams.START, String.valueOf(solrLoggerParameters.getStart()));
        }
		if (solrLoggerParameters.getFacetQueries() != null) {
			for (int i = 0; i < solrLoggerParameters.getFacetQueries().size(); i++) {
				String facetQuery = solrLoggerParameters.getFacetQueries().get(i);
                solrQuery.addFacetQuery(facetQuery);
            }
			if (0 < solrLoggerParameters.getFacetQueries().size()) {
                solrQuery.setFacet(true);
            }
        }


        if (CollectionUtils.isNotEmpty(solrLoggerParameters.getFacetFields())) {
            String facetTerm = null;
            if (solrLoggerParameters.getFacetTerms() != null && solrLoggerParameters.getFacetTerms().size() > 0) {
                StringBuilder _facetTerm = new StringBuilder();
                _facetTerm.append("{!terms=");
                for (int i = 0; i < solrLoggerParameters.getFacetTerms().size(); i++) {
                    if (i > 0) {
                        _facetTerm.append(",");
                    }
                    _facetTerm.append(solrLoggerParameters.getFacetTerms().get(i));
                }
                _facetTerm.append("}");
                facetTerm = _facetTerm.toString();
            }

            for (String facetField : solrLoggerParameters.getFacetFields()) {
                solrQuery.set(FacetParams.FACET_FIELD, (StringUtils.isNotBlank(facetTerm) ? facetTerm : "") + facetField);
            }
            solrQuery.setFacet(true);
        }

        String facetParam = solrQuery.get(FacetParams.FACET);
        if (String.valueOf(true).equals(facetParam)) {

            if (!disableEnum) {
                //Default is facet.method=fc, but very large cores can't handle that method
                solrQuery.setParam("facet.method", "enum");
                int minDf = solrLoggerParameters.getFacetEnumMinDf();
                if (minDf > 0) {
                    solrQuery.setParam("facet.enum.cache.minDf", String.valueOf(minDf));
                }
            }

		solrQuery.setParam(FacetParams.FACET_OFFSET, String.valueOf(solrLoggerParameters.getFacetOffset()));

        // Set the top x of if present
        //Always add the facet limit, since it might as well be -1 (is needed for CUA)
		solrQuery.setFacetLimit(solrLoggerParameters.getMax());

            solrQuery.set(FacetParams.FACET_SORT, solrLoggerParameters.isFacetSort());
            if(solrLoggerParameters.getDateType() == null) {
                solrQuery.setFacetMinCount(solrLoggerParameters.getFacetMinCount());
            }
        }

		if (solrLoggerParameters.getSortField() != null) {
			solrQuery.setSortField(solrLoggerParameters.getSortField(), (solrLoggerParameters.isSortDesc() ? SolrQuery.ORDER.desc : SolrQuery.ORDER.asc));
        }

        // A filter is used instead of a regular query to improve
        // performance and ensure the search result ordering will
        // not be influenced

        // Choose to filter by the Legacy spider IP list (may get too long to properly filter all IP's
        if(ConfigurationManager.getBooleanProperty("solr-statistics", "query.filter.spiderIp",false))
        {
            String spiders = getIgnoreSpiderIPs();
            if (spiders != null && spiders.length() > 0) {
                solrQuery.addFilterQuery(spiders);
            }
        }


        List<String> filterQueries = solrLoggerParameters.getFilterQuery();

        String internalFilterQuery = getFilterQueryForField(filterQueries, CUAConstants.FIELD_IS_INTERNAL);
        if (solrLoggerParameters.isApplyDefaultInternalFilter()) {
            // Choose to filter by isInternal field, may be overriden in future
            // to allow views on stats based on internals.
            if (ConfigurationManager.getBooleanProperty("solr-statistics", "query.filter.isInternal", true) && StringUtils.isBlank(internalFilterQuery)) {
                solrQuery.addFilterQuery("-" + CUAConstants.FIELD_IS_INTERNAL + ":true");
            }
        }
        if (CUAConstants.FQ_IS_INTERNAL_NOOP.equals(internalFilterQuery)) {
            filterQueries.remove(internalFilterQuery);
        }

        String botFilterQuery = getFilterQueryForField(filterQueries, CUAConstants.FIELD_IS_BOT);

		if (solrLoggerParameters.isApplyDefaultBotFilter()) {
        // Choose to filter by isBot field, may be overriden in future
        // to allow views on stats based on bots.
        if(ConfigurationManager.getBooleanProperty("solr-statistics", "query.filter.isBot",true) && StringUtils.isBlank(botFilterQuery))
        {
            solrQuery.addFilterQuery("-" + CUAConstants.FIELD_IS_BOT + ":true");
        }
		}
        if (CUAConstants.FQ_IS_BOT_NOOP.equals(botFilterQuery)) {
            filterQueries.remove(botFilterQuery);
        }

        String bundles;
		if ((bundles = ConfigurationManager.getProperty("solr-statistics", "query.filter.bundles")) != null && 0 < bundles.length() && solrLoggerParameters.isApplyBundleFilter()) {

            /**
             * The code below creates a query that will allow only records which do not have a bundlename
             * (items, collections, ...) or bitstreams that have a configured bundle name
             */
            StringBuffer bundleQuery = new StringBuffer();
            //Also add the possibility that if no bundle name is there these results will also be returned !
            bundleQuery.append("-(bundleName:[* TO *]");
            String[] split = bundles.split(",");
            for (int i = 0; i < split.length; i++) {
                String bundle = split[i].trim();
                bundleQuery.append("-bundleName:").append(bundle);
                if(i != split.length - 1){
                    bundleQuery.append(" AND ");
                }
            }
            bundleQuery.append(")");


            solrQuery.addFilterQuery(bundleQuery.toString());
        }

		if (filterQueries != null) {

            filterQueries = new ArrayList<>(filterQueries); // because a SingletonList does not support removing elements

            for (String query : new ArrayList<>(filterQueries)) { // copy the list for iterating to avoid concurrent modification
                if(StringUtils.isBlank(query)){
                    filterQueries.remove(query);
                }
            }
            if (!filterQueries.isEmpty()) {
                solrQuery.addFilterQuery(filterQueries.toArray(new String[filterQueries.size()]));
            }
        }

        QueryResponse response = null;
        try
        {
            // solr.set
            if (canDisplaySolr) {
				response = solr.query(solrQuery, solrLoggerParameters.getMethod());

                if (log.isDebugEnabled()) {
                    log.debug("SOLR QUERY: " + solrQuery.toString() + " executed in " + response.getQTime() + "ms.");
                }
            } else {
                log.info("SOLR QUERY: " + solrQuery.toString() + " skipped while warming up");

                //Restart out thread
                if(waitWithStart == null || !waitWithStart.isAlive()) {
                    //Add the current one so he isn't empty again
                    queryStack.offer(solrQuery);
                    waitWithStart = new StartupSolrResponseThread();
                    waitWithStart.start();
                }else{
                    //Add our current query to the list he is working on
                    queryStack.offer(solrQuery);
                }

            }
		} catch (SolrServerException e) {
			System.err.println("Error using query " + solrLoggerParameters);
            log.error("Error using query: " + solrQuery.toString(), e);
            throw e;
        }
        return response;
    }

    private static String getFilterQueryForField(List<String> filterQueries, String field) {
        if (filterQueries != null) {
            for (String fq : filterQueries) {
                if (StringUtils.isNotBlank(fq) && fq.matches(".*" + field + "\\s*:.*")) {
                    return fq;
                }
            }
        }
        return null;
    }


	public static InputStream searchJSON(SolrQuery solrQuery, String jsonIdentifier) throws SearchServiceException {
		//We use json as out output type
		solrQuery.setParam("json.nl", "map");
		solrQuery.setParam("json.wrf", jsonIdentifier);
		solrQuery.setParam(CommonParams.WT, "json");

		StringBuilder urlBuilder = new StringBuilder();
		urlBuilder.append(solr.getBaseURL()).append("/select?");
		urlBuilder.append(solrQuery.toString());

		try {
            HttpGet get = new HttpGet(urlBuilder.toString());
            HttpClientBuilder custom = HttpClients.custom();
            CloseableHttpClient client = custom.disableAutomaticRetries().setMaxConnTotal(5).setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(60000).build()).build();
            HttpResponse response = client.execute(get);
			return response.getEntity().getContent();

		} catch (Exception e) {
			log.error("Error while getting json solr result", e);
		}
		return null;
	}

	/**
	 * String of IP and Ranges in IPTable as a Solr Query
	 */
    private static String filterQuery = null;

    /**
     * Returns in a filterQuery string all the ip addresses that should be ignored
     *
     * @return a string query with ip addresses
     */
    public static String getIgnoreSpiderIPs() {
        if (filterQuery == null) {
            StringBuilder query = new StringBuilder();
            boolean first = true;
			for (String ip : SpiderDetector.getSpiderIpAddresses()) {
                if (first) {
                    query.append(" AND ");
                    first = false;
                }

                query.append(" NOT(ip: ").append(ip).append(")");
            }
            filterQuery = query.toString();
        }

        return filterQuery;

    }

    /**
     * Maintenance to keep a SOLR index efficient.
     * Note: This might take a long time.
     */
    public static void optimizeSOLR() {
        try {
            long start = System.currentTimeMillis();
            System.out.println("SOLR Optimize -- Process Started:"+start);
            solr.optimize();
            long finish = System.currentTimeMillis();
            System.out.println("SOLR Optimize -- Process Finished:"+finish);
            System.out.println("SOLR Optimize -- Total time taken:"+(finish-start) + " (ms).");
        } catch (SolrServerException sse) {
            System.err.println(sse.getMessage());
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        }
    }

    public static void shardSolrIndex() throws IOException, SolrServerException {
        /*
        Start by faceting by year so we can include each year in a seperate core !
         */
        SolrQuery yearRangeQuery = new SolrQuery();
        yearRangeQuery.setQuery("*:*");
        yearRangeQuery.setRows(0);
        yearRangeQuery.setFacet(true);
        yearRangeQuery.add(FacetParams.FACET_RANGE, "time");
        //We go back to 2000 the year 2000, this is a bit overkill but this way we ensure we have everything
        //The alternative would be to sort but that isn't recommended since it would be a very costly query !
        yearRangeQuery.add(FacetParams.FACET_RANGE_START, "NOW/YEAR-" + (Calendar.getInstance().get(Calendar.YEAR) - 2000) + "YEARS");
        //Add the +0year to ensure that we DO NOT include the current year
        yearRangeQuery.add(FacetParams.FACET_RANGE_END, "NOW/YEAR+0YEARS");
        yearRangeQuery.add(FacetParams.FACET_RANGE_GAP, "+1YEAR");
        yearRangeQuery.add(FacetParams.FACET_MINCOUNT, String.valueOf(1));

        //Create a temp directory to store our files in !
        File tempDirectory = new File(ConfigurationManager.getProperty("dspace.dir") + File.separator + "temp" + File.separator);
        tempDirectory.mkdirs();


        QueryResponse queryResponse = solr.query(yearRangeQuery);
        //We only have one range query !
        List<RangeFacet.Count> yearResults = queryResponse.getFacetRanges().get(0).getCounts();
        for (RangeFacet.Count count : yearResults) {
            long totalRecords = count.getCount();

            //Create a range query from this !
            //We start with out current year
            DCDate dcStart = new DCDate(count.getValue());
            Calendar endDate = Calendar.getInstance();
            //Advance one year for the start of the next one !
            endDate.setTime(dcStart.toDate());
            endDate.add(Calendar.YEAR, 1);
            DCDate dcEndDate = new DCDate(endDate.getTime());


            StringBuilder filterQuery = new StringBuilder();
            filterQuery.append("time:([");
            filterQuery.append(ClientUtils.escapeQueryChars(dcStart.toString()));
            filterQuery.append(" TO ");
            filterQuery.append(ClientUtils.escapeQueryChars(dcEndDate.toString()));
            filterQuery.append("]");
            //The next part of the filter query excludes the content from midnight of the next year !
            filterQuery.append(" NOT ").append(ClientUtils.escapeQueryChars(dcEndDate.toString()));
            filterQuery.append(")");


            Map<String, String> yearQueryParams = new HashMap<String, String>();
            yearQueryParams.put(CommonParams.Q, "*:*");
            yearQueryParams.put(CommonParams.ROWS, String.valueOf(10000));
            yearQueryParams.put(CommonParams.FQ, filterQuery.toString());
            yearQueryParams.put(CommonParams.WT, "csv");
			yearQueryParams.put("csv.mv.separator", SolrCsvNewFieldProcessor.MULTIPLE_FIELDS_SPLITTER);

            //Start by creating a new core
            String coreName = "statistics-" + dcStart.getYear();
            HttpSolrServer statisticsYearServer = createCore(solr, coreName);

            System.out.println("Moving: " + totalRecords + " into core " + coreName);
            log.info("Moving: " + totalRecords + " records into core " + coreName);

            List<File> filesToUpload = new ArrayList<File>();
            for(int i = 0; i < totalRecords; i+=10000){
                String solrRequestUrl = solr.getBaseURL() + "/select";
                solrRequestUrl = generateURL(solrRequestUrl, yearQueryParams);

                HttpGet get = new HttpGet(solrRequestUrl);
                HttpClientBuilder custom = HttpClients.custom();
                CloseableHttpClient client = custom.disableAutomaticRetries().setMaxConnTotal(5).setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(60000).build()).build();
                HttpResponse response = client.execute(get);
                InputStream csvInputstream = response.getEntity().getContent();
                //Write the csv ouput to a file !
                File csvFile = new File(tempDirectory.getPath() + File.separatorChar + "temp." + dcStart.getYear() + "." + i + ".csv");
                FileUtils.copyInputStreamToFile(csvInputstream, csvFile);
                filesToUpload.add(csvFile);

                //Add 10000 & start over again
                yearQueryParams.put(CommonParams.START, String.valueOf((i + 10000)));
            }

			Set<String> multivaluedFields = getMultivaluedFieldNames();

            for (File tempCsv : filesToUpload) {
                //Upload the data in the csv files to our new solr core
                ContentStreamUpdateRequest contentStreamUpdateRequest = new ContentStreamUpdateRequest("/update/csv");
                contentStreamUpdateRequest.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
                contentStreamUpdateRequest.addFile(tempCsv, "text/plain;charset=utf-8");
                for (String multivaluedField : multivaluedFields) {
                    contentStreamUpdateRequest.setParam("f." + multivaluedField + ".split", Boolean.TRUE.toString());
                    contentStreamUpdateRequest.setParam("f." + multivaluedField + ".separator", SolrCsvNewFieldProcessor.MULTIPLE_FIELDS_SPLITTER);
                    contentStreamUpdateRequest.setParam("f." + multivaluedField + ".encapsulator", "\"");
                }

                statisticsYearServer.request(contentStreamUpdateRequest);
            }
            statisticsYearServer.commit(true, true);


            //Delete contents of this year from our year query !
            solr.deleteByQuery(filterQuery.toString());
            solr.commit(true, true);

            log.info("Moved " + totalRecords + " records into core: " + coreName);
        }

        FileUtils.deleteDirectory(tempDirectory);
    }

    public static HttpSolrServer createCore(HttpSolrServer solr, String coreName) throws IOException, SolrServerException {
        return createCore(solr, coreName, "statistics");
    }

    public static HttpSolrServer createCore(HttpSolrServer solr, String coreName, String solrCoreName) throws IOException, SolrServerException {
        String solrDir = ConfigurationManager.getProperty("dspace.dir") + File.separator + "solr" +File.separator;
        String baseSolrUrl = solr.getBaseURL().replace(solrCoreName, "");
        if (!coreIsActive(solr, coreName, solrCoreName)) {
            CoreAdminRequest.Create create = new CoreAdminRequest.Create();
            create.setCoreName(coreName);
            create.setInstanceDir("statistics");
            create.setDataDir(solrDir + coreName + File.separator + "data");
            HttpSolrServer solrServer = new HttpSolrServer(baseSolrUrl);
            create.process(solrServer);
            log.info("Created core with name: " + coreName);
        } else {
            log.info("Core with name: " + coreName + " already active.");
//            return null;
        }
        return new HttpSolrServer(baseSolrUrl + "/" + coreName);
    }

    public static CoreAdminResponse unloadCore(HttpSolrServer solr, String coreName) throws IOException, SolrServerException {
        return unloadCore(solr, coreName, "statistics");
    }

    public static CoreAdminResponse unloadCore(HttpSolrServer solr, String coreName, String solrCoreName) throws IOException, SolrServerException {
        if (coreIsActive(solr, coreName, solrCoreName)) {
            String baseSolrUrl = solr.getBaseURL().replace(solrCoreName, "");
            HttpSolrServer solrServer = new HttpSolrServer(baseSolrUrl);
            CoreAdminResponse coreAdminResponse = CoreAdminRequest.unloadCore(coreName, solrServer);
            log.info("Unload core with name: " + coreName);
            return coreAdminResponse;
        } else {
            log.info("Can't unload core with name: " + coreName + ". Core not active.");
            return null;
        }
    }

    private static boolean coreIsActive(HttpSolrServer solr, String coreName, String solrCoreName) throws IOException, SolrServerException {
        String baseSolrUrl = solr.getBaseURL().replace(solrCoreName, "");
        HttpSolrServer solrServer = new HttpSolrServer(baseSolrUrl);
        CoreAdminResponse status = CoreAdminRequest.getStatus(coreName, solrServer);
        return status.getUptime(coreName)!=null;
    }



    public static void reindexBitstreamHits(final boolean removeDeletedBitstreams) throws Exception {
        final Context context = new Context();

        visitEachStatisticShard(new ShardVisitor() {
            @Override
            public void visit(HttpSolrServer solr) {
        try {
            //First of all retrieve the total number of records to be updated
            SolrQuery query = new SolrQuery();
            query.setQuery("*:*");
            query.addFilterQuery("type:" + Constants.BITSTREAM);
            //Only retrieve records which do not have a bundle name
            query.addFilterQuery("-bundleName:[* TO *]");
            query.setRows(0);
            addAdditionalSolrYearCores(query);
            long totalRecords = solr.query(query).getResults().getNumFound();

            File tempDirectory = new File(ConfigurationManager.getProperty("dspace.dir") + File.separator + "temp" + File.separator);
            tempDirectory.mkdirs();
            List<File> tempCsvFiles = new ArrayList<File>();
            for(int i = 0; i < totalRecords; i+=10000){
                Map<String, String> params = new HashMap<String, String>();
                params.put(CommonParams.Q, "*:*");
                params.put(CommonParams.FQ, "-bundleName:[* TO *] AND type:" + Constants.BITSTREAM);
                params.put(CommonParams.WT, "csv");
                params.put(CommonParams.ROWS, String.valueOf(10000));
                params.put(CommonParams.START, String.valueOf(i));

                String solrRequestUrl = solr.getBaseURL() + "/select";
                solrRequestUrl = generateURL(solrRequestUrl, params);

                HttpGet get = new HttpGet(solrRequestUrl);
                HttpClientBuilder custom = HttpClients.custom();
                CloseableHttpClient client = custom.disableAutomaticRetries().setMaxConnTotal(5).setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(60000).build()).build();
                HttpResponse response = client.execute(get);

                InputStream  csvOutput = response.getEntity().getContent();
                Reader csvReader = new InputStreamReader(csvOutput);
                List<String[]> rows = new CSVReader(csvReader).readAll();
                String[][] csvParsed = rows.toArray(new String[rows.size()][]);
                String[] header = csvParsed[0];
                //Attempt to find the bitstream id index !
                int idIndex = 0;
                for (int j = 0; j < header.length; j++) {
                    if(header[j].equals("id")){
                        idIndex = j;
                    }
                }

                File tempCsv = new File(tempDirectory.getPath() + File.separatorChar + "temp." + i + ".csv");
                tempCsvFiles.add(tempCsv);
                CSVWriter csvp = new CSVWriter(new FileWriter(tempCsv));
                //csvp.setAlwaysQuote(false);

                //Write the header !
                csvp.writeNext((String[]) ArrayUtils.add(header, "bundleName"));
                Map<Integer, String> bitBundleCache = new HashMap<Integer, String>();
                //Loop over each line (skip the headers though)!
                for (int j = 1; j < csvParsed.length; j++){
                    String[] csvLine = csvParsed[j];
                    //Write the default line !
                    int bitstreamId = Integer.parseInt(csvLine[idIndex]);
                    //Attempt to retrieve our bundle name from the cache !
                    String bundleName = bitBundleCache.get(bitstreamId);
                    if(bundleName == null){
                        //Nothing found retrieve the bitstream
                        Bitstream bitstream = Bitstream.find(context, bitstreamId);
                        //Attempt to retrieve our bitstream !
                        if (bitstream != null){
                            Bundle[] bundles = bitstream.getBundles();
                            if(bundles != null && 0 < bundles.length){
                                Bundle bundle = bundles[0];
                                bundleName = bundle.getName();
                                context.removeCached(bundle, bundle.getID());
                            }else{
                                //No bundle found, we are either a collection or a community logo, check for it !
                                DSpaceObject parentObject = bitstream.getParentObject();
                                if(parentObject instanceof Collection){
                                    bundleName = CUAConstants.BUNDLE_NAME_COLLECTION_LOGO;
                                }else
                                if(parentObject instanceof Community){
                                    bundleName = CUAConstants.BUNDLE_NAME_COMMUNITY_LOGO;
                                }
                                if(parentObject != null){
                                    context.removeCached(parentObject, parentObject.getID());
                                }

                            }
                            //Cache the bundle name
                            bitBundleCache.put(bitstream.getID(), bundleName);
                            //Remove the bitstream from cache
                            context.removeCached(bitstream, bitstreamId);
                        }
                        //Check if we don't have a bundlename
                        //If we don't have one & we do not need to delete the deleted bitstreams ensure that a BITSTREAM_DELETED bundle name is given !
                        if(bundleName == null && !removeDeletedBitstreams){
                            bundleName = CUAConstants.BUNDLE_NAME_BITSTREAM_DELETED;
                        }
                    }
                    csvp.writeNext((String[]) ArrayUtils.add(csvLine, bundleName));
                }

                //Loop over our parsed csv
                csvp.flush();
                csvp.close();
            }

            //Add all the separate csv files
            for (File tempCsv : tempCsvFiles) {
                ContentStreamUpdateRequest contentStreamUpdateRequest = new ContentStreamUpdateRequest("/update/csv");
                contentStreamUpdateRequest.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
                contentStreamUpdateRequest.addFile(tempCsv,"text/plain;charset=utf-8");

                solr.request(contentStreamUpdateRequest);
            }

            //Now that all our new bitstream stats are in place, delete all the old ones !
            solr.deleteByQuery("-bundleName:[* TO *] AND type:" + Constants.BITSTREAM);
            //Commit everything to wrap up
            solr.commit(true, true);
            //Clean up our directory !
            FileUtils.deleteDirectory(tempDirectory);
        } catch (Exception e) {
            log.error("Error while updating the bitstream statistics", e);
                    throw new RuntimeException(e);
        } finally {
            context.abort();
        }

            }
        });

    }

    public static String generateURL(String baseURL, Map<String, String> parameters) throws UnsupportedEncodingException {
        boolean first = true;
        StringBuilder result = new StringBuilder(baseURL);
        for (String key : parameters.keySet())
        {
            if (first)
            {
                result.append("?");
                first = false;
            }
            else
            {
                result.append("&");
            }

            result.append(key).append("=").append(URLEncoder.encode(parameters.get(key), "UTF-8"));
        }

        return result.toString();
    }

    public static void addAdditionalSolrYearCores(SolrQuery solrQuery){
        //Only add if needed
        if(1 < statisticYearCores.size()){
            //The shards are a comma separated list of the urls to the cores
            solrQuery.add(ShardParams.SHARDS, StringUtils.join(statisticYearCores.iterator(), ","));
        }

    }

    public static int getCount() {
        return count;
    }

    public static void setCount(int count) {
        SolrLogger.count = count;
    }

    public static int getCommitLimit() {
        return commit;
    }

    private static class StartupSolrResponseThread extends Thread {

        @Override
        public void run() {
            try {
                int queryCount = ConfigurationManager.getIntProperty("atmire-cua", "solr.statistics.startup.query-success-count");
                int maxResponseTime = ConfigurationManager.getIntProperty("atmire-cua", "solr.statistics.startup.max-response-time");

                while(!queryStack.isEmpty() && !canDisplaySolr){
                    try {
                        SolrQuery solrQuery = queryStack.poll();
                        log.info("Started threaded query, " + queryStack.size() + " waiting");
                        QueryResponse response = solr.query(solrQuery);
                        log.info("Startup query time: " + response.getQTime());
    //                    System.out.println("Startup query time: " + response.getQTime());
                        //Check for a valid response time
                        if(response.getQTime() < maxResponseTime){
                            succeededQueries++;
                            log.info("Startup query time allowed nr of allowed queries:" + succeededQueries);
    //                        System.out.println("Startup query time allowed nr of allowed queries:" + succeededQueries);
                        } else {
                            succeededQueries = 0;
                        }

                        if(succeededQueries == queryCount) {
                            canDisplaySolr = true;
                            queryStack.clear();
                        }


                    } catch (Exception e) {
                        log.error(e);
                    }


                }
            } catch (Throwable t) {
            }
        }
    }


    /**
     * Retrieves a list of all the multi valued fields in the solr
     * @return all fields tagged as multivalued
     * @throws SolrServerException ...
     * @throws IOException ...
     */
    public static Set<String> getMultivaluedFieldNames() throws SolrServerException, IOException {
        Set<String> multivaluedFields = new HashSet<String>();
        LukeRequest lukeRequest = new LukeRequest();
        lukeRequest.setShowSchema(true);
        LukeResponse process = lukeRequest.process(solr);
        Map<String, LukeResponse.FieldInfo> fields = process.getFieldInfo();
        for(String fieldName : fields.keySet())
        {
            LukeResponse.FieldInfo fieldInfo = fields.get(fieldName);
            EnumSet<FieldFlag> flags = fieldInfo.getFlags();
            for(FieldFlag fieldFlag : flags)
            {
                if(fieldFlag.getAbbreviation() == FieldFlag.MULTI_VALUED.getAbbreviation())
                {
                    multivaluedFields.add(fieldName);
                }
            }
        }
        return multivaluedFields;
    }

	public static void commit() throws IOException, SolrServerException {
		solr.commit(false, false);
}

	public static void rollback() throws IOException, SolrServerException {
		solr.rollback();
	}


	public static ExecutorService executor = Executors.newSingleThreadExecutor();

	public static void replaceBitstreamHits(int oldId, int newId) throws Exception {
		ReplaceBitstreamHits replaceBitstreamHits = new ReplaceBitstreamHits(oldId, newId);
		log.info("Replacing bitstream hits: " + replaceBitstreamHits);
		executor.submit(replaceBitstreamHits);

	}

	private static class ReplaceBitstreamHits implements Runnable {
		private int oldId;
		private int newId;

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder("ReplaceBitstreamHits{");
			sb.append("oldId=").append(oldId);
			sb.append(", newId=").append(newId);
			sb.append('}');
			return sb.toString();
		}

		public ReplaceBitstreamHits(int oldId, int newId) {
			this.oldId = oldId;
			this.newId = newId;
		}

		public void run() {


			try {

				update("type:" + Constants.BITSTREAM + " AND id:" + oldId + " AND containerBitstream:" + oldId + " AND bundleName:[* TO *]", "replace", Arrays.asList("id", "containerBitstream"), Arrays.asList(Collections.<Object>singletonList(new Integer(newId).toString()), Collections.<Object>singletonList(new Integer(newId).toString())));
			} catch (Exception e) {
				log.error("Error while updating the bitstream statistics", e);
				throw new RuntimeException(e);
			}

		}
	}
}


