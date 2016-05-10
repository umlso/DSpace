package org.apache.solr.client.solrj.impl;

/**
 * Created by: Art Lowel (art dot lowel at atmire dot com)
 * Date: 09 07 2015
 */
public class Solr1XMLResponseParser extends XMLResponseParser {
    public Solr1XMLResponseParser() {
        super();
    }

    @Override
    public String getContentType() {
        return "text/xml; charset=UTF-8";
    }
}