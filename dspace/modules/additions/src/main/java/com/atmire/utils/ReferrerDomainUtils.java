package com.atmire.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by: Antoine Snyers (antoine at atmire dot com)
 * Date: 12 May 2016
 */
public class ReferrerDomainUtils {

    private static Logger log = Logger.getLogger(ReferrerDomainUtils.class);

    public static String domainForReferrer(String referrer) {
        String domain = null;
        try {
            if (StringUtils.isNotBlank(referrer)) {

//              URI uri = new URI(referrer); // throws too many URISyntaxException

                URL url = new URL(referrer);
                String host = url.getHost();

                if (host != null) {
                    domain = host.startsWith("www.") ? host.substring(4) : host;
                } else {
                    log.warn("Host null for " + referrer);
                }
            }
        } catch (MalformedURLException e) {
            log.error("MalformedURLException (" + e.getMessage() + ") for referrer: " + referrer);
        }
        return domain;
    }
}
