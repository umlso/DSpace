package com.atmire.dspace.statistics.decorators;

import com.atmire.utils.ReferrerDomainUtils;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.dspace.content.DSpaceObject;
import org.dspace.core.Constants;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


@Component("ReferrerDomainDecorator")
public class ReferrerDomainDecorator implements Decorator {

    private static Logger log = Logger.getLogger(ReferrerDomainDecorator.class);

    public List<Integer> getApplicableTypes() {
        List<Integer> types = new ArrayList<>(5);
        types.add(Constants.BITSTREAM);
        types.add(Constants.BUNDLE);
        types.add(Constants.ITEM);
        types.add(Constants.COLLECTION);
        types.add(Constants.COMMUNITY);
        types.add(Constants.SITE);
        return types;
    }

    public void decorate(DSpaceObject dso, SolrInputDocument doc, String statisticType)
            throws SQLException {
        try {
            Object referrerValue = doc.getFieldValue("referrer");
            if (referrerValue instanceof String) {
                String referrer = (String) referrerValue;
                String referrerDomain = ReferrerDomainUtils.domainForReferrer(referrer);
                doc.addField("referrer_domain", referrerDomain);
            }
        } catch (Exception e) {
            log.error("Exception", e);
        }
    }
}