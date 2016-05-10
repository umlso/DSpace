package com.atmire.dspace.rest.common;

import com.atmire.statistics.helpers.Format;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.dspace.core.Context;
import org.dspace.statistics.Dataset;
import org.dspace.statistics.content.Message;
import org.dspace.statistics.content.StatisticsDisplay;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Bavo Van Geit
 * Date: 13/01/15
 * Time: 17:35
 */
@XmlRootElement(name = "statlet")
public class Statlet {
    Logger log = Logger.getLogger(Statlet.class);

    private Dataset dataset;
    private String id;
    private Map<String, Object> variables;
    private Map<String, Object> rendering;
    private Message title;
    private String type;
    private List<Format> formats;
    private String template;
    private boolean outOfDate = false;
    private boolean isUserUpdateRequest = false;
    private Map<String,Map<String,String>> render;
    private List<String> columnLabelOverrides;
    private List<Map<String, String>> columnLabelAttrsOverrides;
    private List<String> rowLabelOverrides;
    private List<Map<String, String>> rowLabelAttrsOverrides;
    private String link;
    private int columns;

    public Statlet() {

    }

    public Statlet(Context context, StatisticsDisplay display, boolean isUserUpdateRequest) throws SQLException, SolrServerException, ParseException, IOException {
        display.setAlwaysCached(!isUserUpdateRequest);
        setUserUpdateRequest(isUserUpdateRequest);
        setDataset((Dataset) display.getDataset(context));
        setOutOfDate(display.needsUpdate(context));
        setType(display.getType());
        setRendering(display.getRenderDescription());
        setTitle(display.getTitle());
        setId(display.getId());
        setFormats(new ArrayList<>(display.getFormats()));
        setTemplate(display.getTemplate());
        setRender(display.getRender());
        Map<String, Object> variables = new HashMap<>();
        variables.put("type",display.getDso_type());
        variables.put("id", display.getDso_id());
        setVariables(variables);
        setColumnLabelOverrides(display.getColumnStatletLabels());
        setColumnLabelAttrsOverrides(display.getColumnStatletLabelsAttrs());
        setRowLabelOverrides(display.getRowStatletLabels());
        setRowLabelAttrsOverrides(display.getRowStatletLabelsAttrs());
        if(getDataset().isHasMore()) {
            setLink(display.getLink());
        }
        setColumns(display.getColumns());
    }

    public Dataset getDataset() {
        return dataset;
    }

    public void setDataset(Dataset dataset) {
        this.dataset = dataset;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String, Object> getRendering() {
        return rendering;
    }

    public void setRendering(Map<String, Object> rendering) {
        this.rendering = rendering;
    }

    public Message getTitle() {
        return title;
    }

    public void setTitle(Message title) {
        this.title = title;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<Format> getFormats() {
        return formats;
    }

    public void setFormats(List<Format> formats) {
        this.formats = formats;
    }

    public String getTemplate() {
        return template;
    }

    public void setTemplate(String template) {
        this.template = template;
    }

    public boolean isOutOfDate() {
        return outOfDate;
    }

    public void setOutOfDate(boolean outOfDate) {
        this.outOfDate = outOfDate;
    }

    public boolean isUserUpdateRequest() {
        return isUserUpdateRequest;
    }

    public void setUserUpdateRequest(boolean isUserUpdateRequest) {
        this.isUserUpdateRequest = isUserUpdateRequest;
    }

    @XmlElement(type=HashMap.class)
    public Map<String, Map<String, String>> getRender() {
        return render;
    }

    public void setRender(Map<String, Map<String, String>> render) {
        this.render = render;
    }

    @XmlElement(type = HashMap.class)
    public Map<String, Object> getVariables() {
        return variables;
    }

    @XmlElement(type = ArrayList.class)
    public List<String> getColumnLabelOverrides() {
        return columnLabelOverrides;
    }

    @XmlElement(type = HashMap.class)
    public List<Map<String, String>> getColumnLabelAttrsOverrides() {
        return columnLabelAttrsOverrides;
    }

    public void setColumnLabelOverrides(List<String> columnLabelOverrides) {
        this.columnLabelOverrides = columnLabelOverrides;
    }

    public void setColumnLabelAttrsOverrides(List<Map<String, String>> columnLabelOverrides) {
        this.columnLabelAttrsOverrides = columnLabelOverrides;
    }

    @XmlElement(type = ArrayList.class)
    public List<String> getRowLabelOverrides() {
        return rowLabelOverrides;
    }

    @XmlElement(type = ArrayList.class)
    public List<Map<String, String>> getRowLabelAttrsOverrides() {
        return rowLabelAttrsOverrides;
    }

    public void setRowLabelOverrides(List<String> rowLabelOverrides) {
        this.rowLabelOverrides = rowLabelOverrides;
    }

    public void setRowLabelAttrsOverrides(List<Map<String, String>> rowLabelOverrides) {
        this.rowLabelAttrsOverrides = rowLabelOverrides;
    }

    public void setVariables(Map<String, Object> variables) {
        this.variables = variables;
    }
    public void addVariables(String name, Object value) {
        if(variables==null){
            this.variables = new HashMap<String, Object>();
        }
        variables.put(name,value);
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public int getColumns() {
        return columns;
    }

    public void setColumns(int columns) {
        this.columns = columns;
    }
}
