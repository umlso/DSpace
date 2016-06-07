package com.atmire.statistics.util.update.atomic.processor;

import com.atmire.statistics.util.update.atomic.AtomicUpdateModifier;
import com.atmire.statistics.util.update.atomic.ProcessingException;
import com.atmire.statistics.util.update.atomic.record.UsageRecord;
import com.atmire.tracing.service.TracingService;
import com.atmire.utils.ReferrerDomainUtils;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrDocument;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by: Antoine Snyers (antoine at atmire dot com)
 * Date: 12 May 2016
 */
public class ReferrerDomainProcessor extends AtomicUpdateProcessor {

    private static Logger log = Logger.getLogger(ReferrerDomainProcessor.class);
    private TracingService tracingService;

    @Autowired
    public void setTracingService(TracingService tracingService) {
        this.tracingService = tracingService;
    }

    @Override
    public void visit(UsageRecord record) throws ProcessingException {
        tracingService.start(this.getClass().getName(), "visit");

        final String REFERRER = "referrer";
        final String REFERRER_DOMAIN = "referrer_domain";

        try {
            SolrDocument sourceDocument = record.getSourceDocument();

            Object referrerValue = sourceDocument.get(REFERRER);
            if (referrerValue instanceof String) {
                String referrer = (String) referrerValue;
                String newReferrerDomain = domainForReferrer(referrer);

                Object referrerDomain = sourceDocument.get(REFERRER_DOMAIN);
                if (referrerDomain != null) {
                    record.addOperation(REFERRER_DOMAIN, AtomicUpdateModifier.ADD, newReferrerDomain);
                } else {
                    record.addOperation(REFERRER_DOMAIN, AtomicUpdateModifier.SET, newReferrerDomain);
                }
            }

        } finally {
            tracingService.stop(this.getClass().getName(), "visit");
        }

    }

    protected String domainForReferrer(String referrer) {
        return ReferrerDomainUtils.domainForReferrer(referrer);
    }
}
