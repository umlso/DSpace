package com.atmire.statistics.util.update.atomic.record;

import com.atmire.statistics.CUAConstants;
import com.atmire.statistics.util.update.atomic.AtomicUpdateModifier;
import com.atmire.statistics.util.update.atomic.ProcessingException;
import com.atmire.statistics.util.update.atomic.processor.RecordVisitor;
import com.atmire.statistics.util.update.atomic.statfields.CommonStatFields;
import com.atmire.statistics.util.update.atomic.statfields.SolrStatsDocumentUtil;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by: Art Lowel (art dot lowel at atmire dot com)
 * Date: 17 06 2015
 *
 * A record represents the original solr document, and
 * the changes that need to happen to it
 */
public abstract class Record implements CommonStatFields {
    private Integer run;
    protected SolrDocument sourceDocument;
    private SolrInputDocument targetDocument;
    private boolean hasOperations = false;

    public Record(SolrDocument sourceDocument, Integer run) {
        this.sourceDocument = sourceDocument;
        this.run = run;
        initTargetDocument();
    }

    private void initTargetDocument() {
        targetDocument = new SolrInputDocument();

        if (getUID() == null) {
            throw new RuntimeException("Atomic updates don't work on documents without a " + CUAConstants.FIELD_UID);
        } else {
            targetDocument.setField(CUAConstants.FIELD_UID, getUID());
        }
    }

    public SolrInputDocument getTargetDocument() {
        return targetDocument;
    }

    public Integer getRun() {
        return run;
    }

    public String getUID() {
        return SolrStatsDocumentUtil.getUID(sourceDocument);
    }

    public Integer getType() {
        return SolrStatsDocumentUtil.getType(sourceDocument);
    }

    public Integer getID() {
        return SolrStatsDocumentUtil.getID(sourceDocument);
    }

    public String getIP() {
        return SolrStatsDocumentUtil.getIP(sourceDocument);
    }

    public Date getTime() {
        return SolrStatsDocumentUtil.getTime(sourceDocument);
    }

    public String getDNS() {
        return SolrStatsDocumentUtil.getDNS(sourceDocument);
    }

    public String getUserAgent() {
        return SolrStatsDocumentUtil.getUserAgent(sourceDocument);
    }

    public boolean needsUpdate() {
        return hasOperations;
    }

    public void addOperation(String fieldName, AtomicUpdateModifier modifier, Object value) {
        value = emptyCollectionsAreNull(value);
        if (needsOperation(fieldName, modifier, value)) {
            Map<String, Object> operationMap = createOperationMap(modifier, value);
            targetDocument.setField(fieldName, operationMap);

            if (!hasOperations) {
                hasOperations = true;
            }
        }
    }

    private Object emptyCollectionsAreNull(Object value) {
        // in solr 4 a field does not get removed if an empty set is used
        // but only when null is used
        if (value instanceof Collection) {
            Iterable iterable = (Iterable) value;
            if (!iterable.iterator().hasNext()) {
                value = null;
            }
        }
        return value;
    }

    private boolean needsOperation(String fieldName, AtomicUpdateModifier modifier, Object newValue) {
        return  isNecessaryAddOrSetOperation(fieldName, modifier, newValue) ||
                isNecessaryRemoveOperation(fieldName, modifier) ||
                isOther(modifier);
    }

    private boolean isNecessaryRemoveOperation(String fieldName, AtomicUpdateModifier modifier) {
        return isRemove(modifier) && hasField(fieldName);
    }

    private boolean isNecessaryAddOrSetOperation(String fieldName, AtomicUpdateModifier modifier, Object newValue) {
        Object sourceValue = sourceDocument.get(fieldName);
        return isAddOrSet(modifier) &&
                !(
                    bothNull(sourceValue, newValue) ||
                    bothEqual(sourceValue, newValue)
                );
    }

    private boolean hasField(String fieldName) {
        return sourceDocument.get(fieldName) != null;
    }

    private boolean isAddOrSet(AtomicUpdateModifier modifier) {
        return modifier == AtomicUpdateModifier.SET || modifier == AtomicUpdateModifier.ADD;
    }

    private boolean isRemove(AtomicUpdateModifier modifier) {
        return modifier == AtomicUpdateModifier.REMOVE || modifier == AtomicUpdateModifier.REMOVE_REGEX;
    }

    private boolean isOther(AtomicUpdateModifier modifier) {
        return !(isAddOrSet(modifier) || isRemove(modifier));
    }

    private boolean bothEqual(Object sourceValue, Object newValue) {
        return sourceValue != null && sourceValue.equals(newValue);
    }

    private boolean bothNull(Object sourceValue, Object newValue) {
        return sourceValue == null && newValue == null;
    }

    private Map<String, Object> createOperationMap(AtomicUpdateModifier modifier, Object value) {
        Map<String, Object> operation = new HashMap<>();
        operation.put(modifier.toString(), value);
        return operation;
    }

    public abstract void accept(RecordVisitor v) throws ProcessingException;

    public String toString() {
        return CUAConstants.FIELD_UID + ": " + getUID();
    }

    public SolrDocument getSourceDocument() {
        return sourceDocument;
    }
}
