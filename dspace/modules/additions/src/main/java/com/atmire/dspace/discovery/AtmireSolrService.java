package com.atmire.dspace.discovery;

import com.atmire.dspace.content.SearchResultDSO;
import com.atmire.dspace.xmlworkflow.XMLWorkflowUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.handler.extraction.ExtractingParams;
import org.dspace.content.*;
import org.dspace.content.Collection;
import org.dspace.content.authority.ChoiceAuthorityManager;
import org.dspace.content.authority.Choices;
import org.dspace.content.authority.MetadataAuthorityManager;
import org.dspace.core.ConfigurationManager;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.core.LogManager;
import org.dspace.discovery.*;
import org.dspace.discovery.configuration.*;
import org.dspace.handle.HandleManager;
import org.dspace.util.MultiFormatDateParser;
import org.dspace.utils.DSpace;
import org.dspace.versioning.VersionHistory;
import org.dspace.versioning.VersioningService;
import org.dspace.xmlworkflow.storedcomponents.XmlWorkflowItem;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by: Art Lowel (art dot lowel at atmire dot com)
 * Date: 23 07 2015
 */
public class AtmireSolrService extends SolrServiceImpl {
    private static final Logger log = Logger.getLogger(AtmireSolrService.class);
    public static final String ITEM_STATUS_FIELD = "item_status_s";
    public static final String ITEM_STATUS_WORKFLOW = "workflow";
    public static final String ITEM_STATUS_ARCHIVED = "archived";
    public static final String WORKFLOW_BATCHEDIT_CLAIMEDBY_FIELD = "workflow_batchedit_claimedby_i";

    private boolean manualCommit = false;

    public boolean isManualCommit() {
        return manualCommit;
    }

    public void setManualCommit(boolean manualCommit) {
        this.manualCommit = manualCommit;
    }

    long time = -1;

    public void logduration(String task) {

        long now = System.currentTimeMillis();

        if (time == -1)
            time = now;

        long duration = now - time;
        log.debug(task + ":" + duration);
        time = now;
    }

    /**
     * Build a Lucene document for a DSpace Item and write the index
     *
     * @param context Users Context
     * @param item    The DSpace Item to be indexed
     * @throws SQLException
     * @throws IOException
     */
    protected void buildDocument(Context context, Item item)
            throws SQLException, IOException {

        logduration("Start Build Document");

        // could be either versioned or unversioned handle, depending on situation
        String handle = item.getHandle();

        if (handle == null) {
            handle = HandleManager.findHandle(context, item);
        }

        // handle should be the versioned handle
        VersionHistory versionHistory = new DSpace().getSingletonService(VersioningService.class).findVersionHistory(context, item.getID());
        if (versionHistory != null) {
            handle = versionHistory.getLatestVersion().getItem().getHandle();
        }

        // unversioned handle
        String handle_unversioned = StringUtils.substringBeforeLast(handle, ".");

        logduration("Got Handle");

        XmlWorkflowItem wfi = null;
        if (XMLWorkflowUtil.usingXMLWorkflow()) {
            wfi = XmlWorkflowItem.findByItem(context, item);
        }

        logduration("Got Workflow Item");

        // get the location string (for searching by collection & community)
        List<String> locations;
        if (wfi == null) {
            locations = getItemLocations(item);
        } else {
            locations = getWFILocations(wfi);
        }

        logduration("Got Locations");

        SolrInputDocument doc = buildDocument(Constants.ITEM, item.getID(), handle,
                locations);

        logduration("Built Initial SolrInputDocument: " + handle);

        doc.addField("handle_unversioned", handle_unversioned);
        doc.addField("withdrawn", item.isWithdrawn());
        doc.addField("discoverable", item.isDiscoverable());

        //Keep a list of our sort values which we added, sort values can only be added once
        List<String> sortFieldsAdded = new ArrayList<String>();
        Set<String> hitHighlightingFields = new HashSet<String>();
        try {
            List<DiscoveryConfiguration> discoveryConfigurations = SearchUtils.getAllDiscoveryConfigurations(item);

            //A map used to save each sidebarFacet config by the metadata fields
            Map<String, List<DiscoverySearchFilter>> searchFilters = new HashMap<String, List<DiscoverySearchFilter>>();
            Map<String, DiscoverySortFieldConfiguration> sortFields = new HashMap<String, DiscoverySortFieldConfiguration>();
            Map<String, DiscoveryRecentSubmissionsConfiguration> recentSubmissionsConfigurationMap = new HashMap<String, DiscoveryRecentSubmissionsConfiguration>();
            Set<String> moreLikeThisFields = new HashSet<String>();
            for (DiscoveryConfiguration discoveryConfiguration : discoveryConfigurations) {
                for (int i = 0; i < discoveryConfiguration.getSearchFilters().size(); i++) {
                    DiscoverySearchFilter discoverySearchFilter = discoveryConfiguration.getSearchFilters().get(i);
                    for (int j = 0; j < discoverySearchFilter.getMetadataFields().size(); j++) {
                        String metadataField = discoverySearchFilter.getMetadataFields().get(j);
                        List<DiscoverySearchFilter> resultingList;
                        if (searchFilters.get(metadataField) != null) {
                            resultingList = searchFilters.get(metadataField);
                        } else {
                            //New metadata field, create a new list for it
                            resultingList = new ArrayList<DiscoverySearchFilter>();
                        }
                        resultingList.add(discoverySearchFilter);

                        searchFilters.put(metadataField, resultingList);
                    }
                }

                DiscoverySortConfiguration sortConfiguration = discoveryConfiguration.getSearchSortConfiguration();
                if (sortConfiguration != null) {
                    for (DiscoverySortFieldConfiguration discoverySortConfiguration : sortConfiguration.getSortFields()) {
                        sortFields.put(discoverySortConfiguration.getMetadataField(), discoverySortConfiguration);
                    }
                }

                DiscoveryRecentSubmissionsConfiguration recentSubmissionConfiguration = discoveryConfiguration.getRecentSubmissionConfiguration();
                if (recentSubmissionConfiguration != null) {
                    recentSubmissionsConfigurationMap.put(recentSubmissionConfiguration.getMetadataSortField(), recentSubmissionConfiguration);
                }

                DiscoveryHitHighlightingConfiguration hitHighlightingConfiguration = discoveryConfiguration.getHitHighlightingConfiguration();
                if (hitHighlightingConfiguration != null) {
                    List<DiscoveryHitHighlightFieldConfiguration> fieldConfigurations = hitHighlightingConfiguration.getMetadataFields();
                    for (DiscoveryHitHighlightFieldConfiguration fieldConfiguration : fieldConfigurations) {
                        hitHighlightingFields.add(fieldConfiguration.getField());
                    }
                }
                DiscoveryMoreLikeThisConfiguration moreLikeThisConfiguration = discoveryConfiguration.getMoreLikeThisConfiguration();
                if (moreLikeThisConfiguration != null) {
                    for (String metadataField : moreLikeThisConfiguration.getSimilarityMetadataFields()) {
                        moreLikeThisFields.add(metadataField);
                    }
                }
            }


            List<String> toProjectionFields = new ArrayList<String>();
            String projectionFieldsString = new DSpace().getConfigurationService().getProperty("discovery.index.projection");
            if (projectionFieldsString != null) {
                if (projectionFieldsString.indexOf(",") != -1) {
                    for (int i = 0; i < projectionFieldsString.split(",").length; i++) {
                        toProjectionFields.add(projectionFieldsString.split(",")[i].trim());
                    }
                } else {
                    toProjectionFields.add(projectionFieldsString);
                }
            }

            logduration("Build Config: " + handle);

            List<String> toIgnoreMetadataFields = SearchUtils.getIgnoredMetadataFields(item.getType());
            Metadatum[] mydc = item.getMetadata(Item.ANY, Item.ANY, Item.ANY, Item.ANY);

            logduration("Get Metadata: " + handle);

            for (Metadatum meta : mydc) {
                String field = meta.schema + "." + meta.element;
                String unqualifiedField = field;

                String value = meta.value;

                if (value == null) {
                    continue;
                }

                if (meta.qualifier != null && !meta.qualifier.trim().equals("")) {
                    field += "." + meta.qualifier;
                }

                //We are not indexing provenance, this is useless
                if (toIgnoreMetadataFields != null && (toIgnoreMetadataFields.contains(field) || toIgnoreMetadataFields.contains(unqualifiedField + "." + Item.ANY))) {
                    continue;
                }

                String authority = null;
                String preferedLabel = null;
                List<String> variants = null;
                boolean isAuthorityControlled = MetadataAuthorityManager
                        .getManager().isAuthorityControlled(meta.schema,
                                meta.element,
                                meta.qualifier);

                int minConfidence = isAuthorityControlled ? MetadataAuthorityManager
                        .getManager().getMinConfidence(
                                meta.schema,
                                meta.element,
                                meta.qualifier) : Choices.CF_ACCEPTED;

                if (isAuthorityControlled && meta.authority != null
                        && meta.confidence >= minConfidence) {
                    boolean ignoreAuthority = new DSpace()
                            .getConfigurationService()
                            .getPropertyAsType(
                                    "discovery.index.authority.ignore." + field,
                                    new DSpace()
                                            .getConfigurationService()
                                            .getPropertyAsType(
                                                    "discovery.index.authority.ignore",
                                                    new Boolean(false)), true);
                    if (!ignoreAuthority) {
                        authority = meta.authority;

                        boolean ignorePrefered = new DSpace()
                                .getConfigurationService()
                                .getPropertyAsType(
                                        "discovery.index.authority.ignore-prefered."
                                                + field,
                                        new DSpace()
                                                .getConfigurationService()
                                                .getPropertyAsType(
                                                        "discovery.index.authority.ignore-prefered",
                                                        new Boolean(false)),
                                        true);
                        if (!ignorePrefered) {

                            preferedLabel = ChoiceAuthorityManager.getManager()
                                    .getLabel(meta.schema, meta.element,
                                            meta.qualifier, meta.authority,
                                            meta.language);
                        }

                        boolean ignoreVariants = new DSpace()
                                .getConfigurationService()
                                .getPropertyAsType(
                                        "discovery.index.authority.ignore-variants."
                                                + field,
                                        new DSpace()
                                                .getConfigurationService()
                                                .getPropertyAsType(
                                                        "discovery.index.authority.ignore-variants",
                                                        new Boolean(false)),
                                        true);
                        if (!ignoreVariants) {
                            variants = ChoiceAuthorityManager.getManager()
                                    .getVariants(meta.schema, meta.element,
                                            meta.qualifier, meta.authority,
                                            meta.language);
                        }

                    }
                }

                if ((searchFilters.get(field) != null || searchFilters.get(unqualifiedField + "." + Item.ANY) != null)) {
                    List<DiscoverySearchFilter> searchFilterConfigs = searchFilters.get(field);
                    if (searchFilterConfigs == null) {
                        searchFilterConfigs = searchFilters.get(unqualifiedField + "." + Item.ANY);
                    }

                    for (DiscoverySearchFilter searchFilter : searchFilterConfigs) {
                        Date date = null;
                        String separator = new DSpace().getConfigurationService().getProperty("discovery.solr.facets.split.char");
                        if (separator == null) {
                            separator = FILTER_SEPARATOR;
                        }
                        if (searchFilter.getType().equals(DiscoveryConfigurationParameters.TYPE_DATE)) {
                            //For our search filters that are dates we format them properly
                            date = MultiFormatDateParser.parse(value);
                            if (date != null) {
                                //TODO: make this date format configurable !
                                value = DateFormatUtils.formatUTC(date, "yyyy-MM-dd");
                            }
                        }
                        doc.addField(searchFilter.getIndexFieldName(), value);
                        doc.addField(searchFilter.getIndexFieldName() + "_keyword", value);

                        if (authority != null && preferedLabel == null) {
                            doc.addField(searchFilter.getIndexFieldName()
                                    + "_keyword", value + AUTHORITY_SEPARATOR
                                    + authority);
                            doc.addField(searchFilter.getIndexFieldName()
                                    + "_authority", authority);
                            doc.addField(searchFilter.getIndexFieldName()
                                    + "_acid", value.toLowerCase()
                                    + separator + value
                                    + AUTHORITY_SEPARATOR + authority);
                        }

                        if (preferedLabel != null) {
                            doc.addField(searchFilter.getIndexFieldName(),
                                    preferedLabel);
                            doc.addField(searchFilter.getIndexFieldName()
                                    + "_keyword", preferedLabel);
                            doc.addField(searchFilter.getIndexFieldName()
                                    + "_keyword", preferedLabel
                                    + AUTHORITY_SEPARATOR + authority);
                            doc.addField(searchFilter.getIndexFieldName()
                                    + "_authority", authority);
                            doc.addField(searchFilter.getIndexFieldName()
                                    + "_acid", preferedLabel.toLowerCase()
                                    + separator + preferedLabel
                                    + AUTHORITY_SEPARATOR + authority);
                        }
                        if (variants != null) {
                            for (String var : variants) {
                                doc.addField(searchFilter.getIndexFieldName() + "_keyword", var);
                                doc.addField(searchFilter.getIndexFieldName()
                                        + "_acid", var.toLowerCase()
                                        + separator + var
                                        + AUTHORITY_SEPARATOR + authority);
                            }
                        }

                        //Add a dynamic fields for auto complete in search
                        doc.addField(searchFilter.getIndexFieldName() + "_ac",
                                value.toLowerCase() + separator + value);
                        if (preferedLabel != null) {
                            doc.addField(searchFilter.getIndexFieldName()
                                    + "_ac", preferedLabel.toLowerCase()
                                    + separator + preferedLabel);
                        }
                        if (variants != null) {
                            for (String var : variants) {
                                doc.addField(searchFilter.getIndexFieldName()
                                        + "_ac", var.toLowerCase() + separator
                                        + var);
                            }
                        }

                        if (searchFilter.getFilterType().equals(DiscoverySearchFilterFacet.FILTER_TYPE_FACET)) {
                            if (searchFilter.getType().equals(DiscoveryConfigurationParameters.TYPE_TEXT)) {
                                //Add a special filter
                                //We use a separator to split up the lowercase and regular case, this is needed to get our filters in regular case
                                //Solr has issues with facet prefix and cases
                                if (authority != null) {
                                    String facetValue = preferedLabel != null ? preferedLabel : value;
                                    doc.addField(searchFilter.getIndexFieldName() + "_filter", facetValue.toLowerCase() + separator + facetValue + AUTHORITY_SEPARATOR + authority);
                                } else
                            	{

									// TODO: if one were wanting to add normalization to the casing of the indexed value, one would do so here, with "value"
									// you could even do it and pretend it's an "authority" by using the AUTHORITY_SEPARATOR as above

                                    doc.addField(searchFilter.getIndexFieldName() + "_filter", value.toLowerCase() + separator + value);
                                }
                            } else if (searchFilter.getType().equals(DiscoveryConfigurationParameters.TYPE_DATE)) {
                                if (date != null) {
                                    String indexField = searchFilter.getIndexFieldName() + ".year";
                                    String yearUTC = DateFormatUtils.formatUTC(date, "yyyy");
                                    doc.addField(searchFilter.getIndexFieldName() + "_keyword", yearUTC);
                                    // add the year to the autocomplete index
                                    doc.addField(searchFilter.getIndexFieldName() + "_ac", yearUTC);
                                    doc.addField(indexField, yearUTC);

                                    if (yearUTC.startsWith("0")) {
                                        doc.addField(
                                                searchFilter.getIndexFieldName()
                                                        + "_keyword",
                                                yearUTC.replaceFirst("0*", ""));
                                        // add date without starting zeros for autocomplete e filtering
                                        doc.addField(
                                                searchFilter.getIndexFieldName()
                                                        + "_ac",
                                                yearUTC.replaceFirst("0*", ""));
                                        doc.addField(
                                                searchFilter.getIndexFieldName()
                                                        + "_ac",
                                                value.replaceFirst("0*", ""));
                                        doc.addField(
                                                searchFilter.getIndexFieldName()
                                                        + "_keyword",
                                                value.replaceFirst("0*", ""));
                                    }

                                    //Also save a sort value of this year, this is required for determining the upper & lower bound year of our facet
                                    if (doc.getField(indexField + "_sort") == null) {
                                        //We can only add one year so take the first one
                                        doc.addField(indexField + "_sort", yearUTC);
                                    }
                                }
                            } else if (searchFilter.getType().equals(DiscoveryConfigurationParameters.TYPE_HIERARCHICAL)) {
                                HierarchicalSidebarFacetConfiguration hierarchicalSidebarFacetConfiguration = (HierarchicalSidebarFacetConfiguration) searchFilter;
                                String[] subValues = value.split(hierarchicalSidebarFacetConfiguration.getSplitter());
                                if (hierarchicalSidebarFacetConfiguration.isSkipFirstNodeLevel() && 1 < subValues.length) {
                                    //Remove the first element of our array
                                    subValues = (String[]) ArrayUtils.subarray(subValues, 1, subValues.length);
                                }
                                for (int i = 0; i < subValues.length; i++) {
                                    StringBuilder valueBuilder = new StringBuilder();
                                    for (int j = 0; j <= i; j++) {
                                        valueBuilder.append(subValues[j]);
                                        if (j < i) {
                                            valueBuilder.append(hierarchicalSidebarFacetConfiguration.getSplitter());
                                        }
                                    }

                                    String indexValue = valueBuilder.toString().trim();
                                    doc.addField(searchFilter.getIndexFieldName() + "_tax_" + i + "_filter", indexValue.toLowerCase() + separator + indexValue);
                                    //We add the field x times that it has occurred
                                    for (int j = i; j < subValues.length; j++) {
                                        doc.addField(searchFilter.getIndexFieldName() + "_filter", indexValue.toLowerCase() + separator + indexValue);
                                        doc.addField(searchFilter.getIndexFieldName() + "_keyword", indexValue);
                                    }
                                }
                            }
                        }
                    }
                }

                if ((sortFields.get(field) != null || recentSubmissionsConfigurationMap.get(field) != null) && !sortFieldsAdded.contains(field)) {
                    //Only add sort value once
                    String type;
                    if (sortFields.get(field) != null) {
                        type = sortFields.get(field).getType();
                    } else {
                        type = recentSubmissionsConfigurationMap.get(field).getType();
                    }


                    if(type.equals(DiscoveryConfigurationParameters.TYPE_DATE) && value != null)
                    {
                        Date date = MultiFormatDateParser.parse(value);
                        if (date != null) {
                            doc.addField(field + "_dt", date);
                        }
						// skip any errors here and ignore them, they are not important
                    } else {
                        doc.addField(field + "_sort", value);
                    }
                    sortFieldsAdded.add(field);
                }

                if (hitHighlightingFields.contains(field) || hitHighlightingFields.contains("*") || hitHighlightingFields.contains(unqualifiedField + "." + Item.ANY)) {
                    doc.addField(field + "_hl", value);
                }

                if (moreLikeThisFields.contains(field) || moreLikeThisFields.contains(unqualifiedField + "." + Item.ANY)) {
                    doc.addField(field + "_mlt", value);
                }

                doc.addField(field, value);
                if (toProjectionFields.contains(field) || toProjectionFields.contains(unqualifiedField + "." + Item.ANY)) {
                    StringBuffer variantsToStore = new StringBuffer();
                    if (variants != null) {
                        for (String var : variants) {
                            variantsToStore.append(VARIANTS_STORE_SEPARATOR);
                            variantsToStore.append(var);
                        }
                    }
                    doc.addField(
                            field + "_stored",
                            value + STORE_SEPARATOR + preferedLabel
                                    + STORE_SEPARATOR
                                    + (variantsToStore.length() > VARIANTS_STORE_SEPARATOR
                                    .length() ? variantsToStore
                                    .substring(VARIANTS_STORE_SEPARATOR
                                            .length()) : "null")
                                    + STORE_SEPARATOR + authority
                                    + STORE_SEPARATOR + meta.language);
                }

                if (meta.language != null && !meta.language.trim().equals("")) {
                    String langField = field + "." + meta.language;
                    doc.addField(langField, value);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }


        if (wfi == null) {
            doc.addField(ITEM_STATUS_FIELD, ITEM_STATUS_ARCHIVED);
        } else {
            doc.addField(ITEM_STATUS_FIELD, ITEM_STATUS_WORKFLOW);

            Metadatum[] claimedVals = item.getMetadata("workflow", "batchedit", "claimedby", Item.ANY);
            if (0 < claimedVals.length) {
                String user = claimedVals[0].value;
                doc.addField(WORKFLOW_BATCHEDIT_CLAIMEDBY_FIELD, user);
            }
        }

        logduration("Added Metadata: " + handle);

        try {

            Metadatum[] values = item.getMetadataByMetadataString("dc.relation.ispartof");

            if (values != null && values.length > 0 && values[0] != null && values[0].value != null) {
                // group on parent
                String handlePrefix = ConfigurationManager.getProperty("handle.canonical.prefix");
                if (handlePrefix == null || handlePrefix.length() == 0) {
                    handlePrefix = "http://hdl.handle.net/";
                }

                doc.addField("publication_grp", values[0].value.replaceFirst(handlePrefix, ""));

            } else {
                // group on self
                doc.addField("publication_grp", item.getHandle());
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }


        logduration("Added Grouping: " + handle);


        List<BitstreamContentStream> streams = new ArrayList<BitstreamContentStream>();

        try {
            // now get full text of any bitstreams in the TEXT bundle
            // trundle through the bundles
            Bundle[] myBundles = item.getBundles();

            for (Bundle myBundle : myBundles) {
                if ((myBundle.getName() != null)
                        && myBundle.getName().equals("TEXT")) {
                    // a-ha! grab the text out of the bitstreams
                    Bitstream[] myBitstreams = myBundle.getBitstreams();

                    for (Bitstream myBitstream : myBitstreams) {
                        try {

                            streams.add(new BitstreamContentStream(myBitstream));

                            log.debug("  Added BitStream: "
                                    + myBitstream.getStoreNumber() + "	"
                                    + myBitstream.getSequenceID() + "   "
                                    + myBitstream.getName());

                        } catch (Exception e) {
                            // this will never happen, but compiler is now
                            // happy.
                            log.trace(e.getMessage(), e);
                        }
                    }
                }
            }
        } catch (RuntimeException e) {
            log.error(e.getMessage(), e);
        }
        logduration("Created and Added Bitstreams: " + handle);

        //Do any additional indexing, depends on the plugins
        List<SolrServiceIndexPlugin> solrServiceIndexPlugins = new DSpace().getServiceManager().getServicesByType(SolrServiceIndexPlugin.class);
        for (SolrServiceIndexPlugin solrServiceIndexPlugin : solrServiceIndexPlugins) {
            solrServiceIndexPlugin.additionalIndex(context, item, doc);

            logduration("Processed " + solrServiceIndexPlugin.getClass().getName() + " : " + handle);

        }

        // write the index and close the inputstreamreaders
        try {
            writeDocument(doc, streams);
            logduration("Wrote Item: " + handle + " to Index");
        } catch (RuntimeException e) {
            log.error("Error while writing item to discovery index: " + handle + " message:" + e.getMessage(), e);
        }
    }

    /**
     * @param wfi the workflowitem for which our locations are to be retrieved
     * @return a list containing the identifiers of the communities & collections
     * @throws SQLException sql exception
     */
    protected List<String> getWFILocations(InProgressSubmission wfi)
            throws SQLException {
        List<String> locations = new Vector<String>();

        org.dspace.content.Collection collection = wfi.getCollection();

        // build list of community ids
        Community[] communities = collection.getCommunities();

        // now put those into strings
        int i = 0;

        for (i = 0; i < communities.length; i++) {
            locations.add("m" + communities[i].getID());
        }

        locations.add("l" + collection.getID());

        return locations;
    }

    /**
     * Write the document to the index under the appropriate handle.
     *
     * @param doc     the solr document to be written to the server
     * @param streams
     * @throws IOException IO exception
     */
    protected void writeDocument(SolrInputDocument doc, List<BitstreamContentStream> streams)
            throws IOException {

        try {
            if (getSolr() != null) {
                if (CollectionUtils.isNotEmpty(streams)) {
                    log.debug("calling Extracting Request Handler");
                    ContentStreamUpdateRequest req = new ContentStreamUpdateRequest("/update/extract");

                    for (BitstreamContentStream bce : streams) {
                        req.addContentStream(bce);
                    }

                    ModifiableSolrParams params = new ModifiableSolrParams();

                    //req.setParam(ExtractingParams.EXTRACT_ONLY, "true");
                    for (String name : doc.getFieldNames()) {
                        for (Object val : doc.getFieldValues(name)) {
                            params.add(ExtractingParams.LITERALS_PREFIX + name, val.toString());
                        }
                    }

                    req.setParams(params);
                    req.setParam(ExtractingParams.UNKNOWN_FIELD_PREFIX, "attr_");
                    req.setParam(ExtractingParams.MAP_PREFIX + "content", "fulltext");
                    req.setParam(ExtractingParams.EXTRACT_FORMAT, "text");

                    if (isManualCommit()) {
                        //Commit and do not wait for the search and/or the current buffer to be flushed to disk.
                        req.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, false);
                        req.setParam(UpdateParams.OPEN_SEARCHER, "true");
                        log.debug("commit: true");
                    }

                    req.process(getSolr());
                } else {
                    log.debug("calling Default Request Handler");
                    getSolr().add(doc);
                }
                logduration("Added to Solr");
            }
        } catch (SolrServerException e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * If the handle for the "dso" already exists in the index, and the "dso"
     * has a lastModified timestamp that is newer than the document in the index
     * then it is updated, otherwise a new document is added.
     *
     * @param context Users Context
     * @param dso     DSpace Object (Item, Collection or Community
     * @param force   Force update even if not stale.
     * @throws SQLException
     * @throws IOException
     */
    @Override
    public void indexContent(Context context, DSpaceObject dso,
            boolean force) throws SQLException {

        String handle = dso.getHandle();

        if (handle == null) {
            handle = HandleManager.findHandle(context, dso);
        }

        try {
            switch (dso.getType()) {
                case Constants.ITEM:
                    Item item = (Item) dso;
                    if (item.isArchived() || item.isWithdrawn()) {
                        /**
                         * If the item is in the repository now, add it to the index
                         */
                        if (requiresIndexing(handle, ((Item) dso).getLastModified())
                                || force) {
                            logduration("Check RequiresIndexing: " + item.getHandle());

                            unIndexContent(context, handle);

                            logduration("UnIndex: " + item.getHandle());

                            buildDocument(context, (Item) dso);
                        }
                    } else {
                        /**
                         * Make sure the item is not in the index if it is not in
                         * archive or withwrawn.
                         */
                        unIndexContent(context, item);
                        logduration("UnIndex: " + item.getHandle());
                        log.info("Removed Item: " + handle + " from Index");
                    }
                    break;

                case Constants.COLLECTION:
                    buildDocument(context, (Collection) dso);
                    log.info("Wrote Collection: " + handle + " to Index");
                    break;

                case Constants.COMMUNITY:
                    buildDocument(context, (Community) dso);
                    log.info("Wrote Community: " + handle + " to Index");
                    break;

                default:
                    log
                            .error("Only Items, Collections and Communities can be Indexed");
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Iterates over all Items, Collections and Communities. And updates them in
     * the index. Uses decaching to control memory footprint. Uses indexContent
     * and isStale to check state of item in index.
     * <p/>
     * At first it may appear counterintuitive to have an IndexWriter/Reader
     * opened and closed on each DSO. But this allows the UI processes to step
     * in and attain a lock and write to the index even if other processes/jvms
     * are running a reindex.
     *
     * @param context the dspace context
     * @param force   whether or not to force the reindexing
     */
    @Override
    public void updateIndex(Context context, boolean force) {
        try {
            logduration("Start Entire Index");

            // Only retrieve Item id at this point.
            ItemIdIterator itemIDs = ItemIdIterator.findAllUnfilteredItemIds(context);
            logduration("Gathered Item IDs");

            int count = 0;

            try {
                // Process each Item and completely cleanup database connection
                while (itemIDs.hasNext()) {
                    Integer id = itemIDs.next();

                    log.info("Processing (" + count++ + " of " + itemIDs.getTotal() + "): " + id);
                    Context tempctx = new Context();
                    tempctx.turnOffAuthorisationSystem();

                    try {
                        indexContent(tempctx, Item.find(tempctx, id), force);
                    } finally {
                        tempctx.complete();
                    }

                    logduration("completed: " + id);
                }
            } finally {
                itemIDs.close();
            }

            if (XMLWorkflowUtil.usingXMLWorkflow()) {
                XmlWorkflowItem[] workflowItems = XmlWorkflowItem.findAll(context);
                for (XmlWorkflowItem workflowItem : workflowItems) {
                    indexContent(context, workflowItem.getItem(), force);
                    workflowItem.getItem().decache();
                }
            }

            org.dspace.content.Collection[] collections = org.dspace.content.Collection.findAll(context);
            for (org.dspace.content.Collection collection : collections) {
                indexContent(context, collection, force);
                context.removeCached(collection, collection.getID());

            }

            Community[] communities = Community.findAll(context);
            for (Community community : communities) {
                indexContent(context, community, force);
                context.removeCached(community, community.getID());
            }

            //Call commit method to support disabling commit..
            commit();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    protected DiscoverResult retrieveResult(Context context, DiscoverQuery query, QueryResponse solrQueryResponse)
            throws SQLException {
        DiscoverResult result = new DiscoverResult();

        if (solrQueryResponse != null) {
            result.setSearchTime(solrQueryResponse.getQTime());
            result.setStart(query.getStart());
            result.setMaxResults(query.getMaxResults());
            result.setTotalSearchResults(solrQueryResponse.getResults().getNumFound());

            List<String> searchFields = query.getSearchFields();
            List<String> database_lookup_property = query.getProperties().get("database_lookup");
            boolean databaseLookup = database_lookup_property == null || database_lookup_property.isEmpty() || !database_lookup_property.get(0).equalsIgnoreCase("false");
            for (SolrDocument doc : solrQueryResponse.getResults()) {
                DSpaceObject dso;
                if (databaseLookup) {
                    dso = findDSpaceObject(context, doc);
                } else {
                    dso = new SearchResultDSO(context, doc);
                }

                if (dso != null) {
                    result.addDSpaceObject(dso);
                } else {
                    log.error(LogManager.getHeader(context, "Error while retrieving DSpace object from discovery index", "Handle: " + doc.getFirstValue("handle")));
                    continue;
                }

                DiscoverResult.SearchDocument resultDoc = new DiscoverResult.SearchDocument();
                //Add information about our search fields
                for (String field : searchFields) {
                    List<String> valuesAsString = new ArrayList<String>();
                    for (Object o : doc.getFieldValues(field)) {
                        valuesAsString.add(String.valueOf(o));
                    }
                    resultDoc.addSearchField(field, valuesAsString.toArray(new String[valuesAsString.size()]));
                }
                result.addSearchDocument(dso, resultDoc);

                if (solrQueryResponse.getHighlighting() != null) {
                    Map<String, List<String>> highlightedFields = solrQueryResponse.getHighlighting().get(dso.getType() + "-" + dso.getID());
                    if (MapUtils.isNotEmpty(highlightedFields)) {
                        //We need to remove all the "_hl" appendix strings from our keys
                        Map<String, List<String>> resultMap = new HashMap<String, List<String>>();
                        for (String key : highlightedFields.keySet()) {
                            resultMap.put(key.substring(0, key.lastIndexOf("_hl")), highlightedFields.get(key));
                        }

                        result.addHighlightedResult(dso, new DiscoverResult.DSpaceObjectHighlightResult(dso, resultMap));
                    }
                }
            }

            //Resolve our facet field values
            List<FacetField> facetFields = solrQueryResponse.getFacetFields();
            if (facetFields != null) {
                for (int i = 0; i < facetFields.size(); i++) {
                    FacetField facetField = facetFields.get(i);
                    DiscoverFacetField facetFieldConfig = query.getFacetFields().get(i);
                    List<FacetField.Count> facetValues = facetField.getValues();
                    if (facetValues != null) {
                        if (facetFieldConfig.getType().equals(DiscoveryConfigurationParameters.TYPE_DATE) && facetFieldConfig.getSortOrder().equals(DiscoveryConfigurationParameters.SORT.VALUE)) {
                            //If we have a date & are sorting by value, ensure that the results are flipped for a proper result
                            Collections.reverse(facetValues);
                        }

                        for (FacetField.Count facetValue : facetValues) {
                            String displayedValue = transformDisplayedValue(context, facetField.getName(), facetValue.getName());
                            String field = transformFacetField(facetFieldConfig, facetField.getName(), true);
                            String authorityValue = transformAuthorityValue(context, facetField.getName(), facetValue.getName());
                            String sortValue = transformSortValue(context, facetField.getName(), facetValue.getName());
                            String filterValue = displayedValue;
                            if (StringUtils.isNotBlank(authorityValue)) {
                                filterValue = authorityValue;
                            }
                            result.addFacetResult(
                                    field,
                                    new DiscoverResult.FacetResult(filterValue,
                                            displayedValue, authorityValue,
                                            sortValue, facetValue.getCount()));
                        }
                    }
                }
            }

            if (solrQueryResponse.getFacetQuery() != null) {
                // just retrieve the facets in the order they where requested!
                // also for the date we ask it in proper (reverse) order
                // At the moment facet queries are only used for dates
                LinkedHashMap<String, Integer> sortedFacetQueries = new LinkedHashMap<String, Integer>(solrQueryResponse.getFacetQuery());
                for (String facetQuery : sortedFacetQueries.keySet()) {
                    //TODO: do not assume this, people may want to use it for other ends, use a regex to make sure
                    //We have a facet query, the values looks something like: dateissued.year:[1990 TO 2000] AND -2000
                    //Prepare the string from {facet.field.name}:[startyear TO endyear] to startyear - endyear
                    String facetField = facetQuery.substring(0, facetQuery.indexOf(":"));
                    String name = "";
                    String filter = "";
                    if (facetQuery.indexOf('[') > -1 && facetQuery.lastIndexOf(']') > -1) {
                        name = facetQuery.substring(facetQuery.indexOf('[') + 1);
                        name = name.substring(0, name.lastIndexOf(']')).replaceAll("TO", "-");
                        filter = facetQuery.substring(facetQuery.indexOf('['));
                        filter = filter.substring(0, filter.lastIndexOf(']') + 1);
                    }

                    Integer count = sortedFacetQueries.get(facetQuery);

                    //No need to show empty years
                    if (0 < count) {
                        result.addFacetResult(facetField, new DiscoverResult.FacetResult(filter, name, null, name, count));
                    }
                }
            }

            if (solrQueryResponse.getSpellCheckResponse() != null) {
                String recommendedQuery = solrQueryResponse.getSpellCheckResponse().getCollatedResult();
                if (StringUtils.isNotBlank(recommendedQuery)) {
                    result.setSpellCheckQuery(recommendedQuery);
                }
            }
        }

        return result;
    }

    protected String transformAuthorityValue(Context context, String field, String value)
            throws SQLException {
        if (field.startsWith("location.coll")) {
            return value;
        }
        return super.transformAuthorityValue(context, field, value);
    }

    public void commit() throws SearchServiceException {
        try {
            if (isManualCommit() && getSolr() != null) {
                //Commit and do not wait for the search and/or the current buffer to be flushed to disk.
                getSolr().commit(false, false);

                logduration("Commit");
            } else {
                log.debug("commit disabled");
            }
        } catch (Exception e) {
            throw new SearchServiceException(e.getMessage(), e);
        }
    }
}