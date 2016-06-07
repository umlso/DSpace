package com.atmire.statistics;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author kevinvandevelde at atmire.com
 * Date: 21-jan-2009
 * Time: 13:44:48
 *
 */
public abstract class AbstractDataset {

    public static int DATASET = 1;
    protected static int STRING_DATASET = 2;

    public static int ROW = 1;
    public static int COLL = 2;

    protected int nbRows;
    protected int nbCols;
    /* The labels shown in our columns */
    protected List<String> colLabels;
    /* The labels shown in our rows */
    protected List<String> rowLabels;
    protected String colTitle;
    protected String rowTitle;
    /* The attributes for the colls */
    protected List<Map<String, String>> colLabelsAttrs;
    /* The attributes for the rows */
    protected List<Map<String, String>> rowLabelsAttrs;
    protected Map<String,Map<String, String>> cellAttrs;
    /* The format in which we format our floats */
    protected String format = "0";
    private boolean hasMore;
    protected String formatLocale = CUAConstants.DECIMAL_FORMAT_LOCALE.toString();

    public AbstractDataset(int rows, int cols){
        nbRows = rows;
        nbCols = cols;
        initColumnLabels(cols);
        initRowLabels(rows);
    }

    public AbstractDataset(){
    }

    protected void initRowLabels(int rows) {
        rowLabels = new ArrayList<>(rows);
        rowLabelsAttrs = new ArrayList<Map<String, String>>();
        for (int i = 0; i < rows; i++) {
            rowLabels.add("Row " + (i+1));
            rowLabelsAttrs.add(new HashMap<String, String>());
        }
    }

    protected void initColumnLabels(int nbCols) {
        colLabels = new ArrayList<>(nbCols);
        colLabelsAttrs = new ArrayList<>();
        for (int i = 0; i < nbCols; i++) {
            colLabels.add("Column " + (i+1));
            colLabelsAttrs.add(new HashMap<String, String>());
        }
    }

    public void setColLabel(int n, String label){
        colLabels.set(n, label);
    }

    public void setRowLabel(int n, String label){
        rowLabels.set(n, label);
    }

    public String getRowTitle() {
        return rowTitle;
    }

    public String getColTitle() {
        return colTitle;
    }

    public void setColTitle(String colTitle) {
        this.colTitle = colTitle;
    }


    public void setRowTitle(String rowTitle) {
        this.rowTitle = rowTitle;
    }

    public void setRowLabelAttr(int pos, String attrName, String attr){
        Map<String, String> attrs = rowLabelsAttrs.get(pos);
        attrs.put(attrName, attr);
        rowLabelsAttrs.set(pos, attrs);
    }

    public void setRowLabelAttr(int pos, Map<String, String> attrMap){
        rowLabelsAttrs.set(pos, attrMap);
    }

    public void setColLabelAttr(int pos, String attrName, String attr){
        Map<String, String> attrs = colLabelsAttrs.get(pos);
        attrs.put(attrName, attr);
        colLabelsAttrs.set(pos, attrs);
    }

    public void setColLabelAttr(int pos, Map<String, String> attrMap) {
        colLabelsAttrs.set(pos, attrMap);
    }

    public void setCellAttr(int row, int coll, String attr, String value) {
        if(cellAttrs==null){
            cellAttrs = new HashMap<String,Map<String, String>>();
        }
        Map<String, String> attrMap = cellAttrs.get(row + ":" + coll);
        if(attrMap==null){
            attrMap = new HashMap<String, String>();
        }
        attrMap.put(attr,value);
        cellAttrs.put(row + ":" + coll, attrMap);
    }


    public List<Map<String, String>> getColLabelsAttrs() {
        return colLabelsAttrs;
    }

    public List<Map<String, String>> getRowLabelsAttrs() {
        return rowLabelsAttrs;
    }

    public List<String> getColLabels() {
        return colLabels;
    }

    public List<String> getRowLabels() {
        return rowLabels;
    }

    public int getNbRows() {
        return nbRows;
    }

    public int getNbCols() {
        return nbCols;
    }

    public String getFormatLocale(){
        return formatLocale;
    }

    public void setFormatLocale(String formatLocale) {
        this.formatLocale = formatLocale;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    @JsonIgnore
    public abstract int getType();

    @JsonAnySetter
    private void setNbRows(int nbRows) {
        this.nbRows = nbRows;
    }

    @JsonAnySetter
    private void setNbCols(int nbCols) {
        this.nbCols = nbCols;
    }

    @JsonAnySetter
    private void setColLabels(List<String> colLabels) {
        this.colLabels = colLabels;
    }

    @JsonAnySetter
    private void setRowLabels(List<String> rowLabels) {
        this.rowLabels = rowLabels;
    }

    @JsonAnySetter
    private void setColLabelsAttrs(List<Map<String, String>> colLabelsAttrs) {
        this.colLabelsAttrs = colLabelsAttrs;
    }

    @JsonAnySetter
    private void setRowLabelsAttrs(List<Map<String, String>> rowLabelsAttrs) {
        this.rowLabelsAttrs = rowLabelsAttrs;
    }

    public Map<String, Map<String, String>> getCellAttrs() {
        return cellAttrs;
    }
    @JsonAnySetter
    private void setCellAttrs(Map<String, Map<String, String>> cellAttrs) {
        this.cellAttrs = cellAttrs;
    }

    public boolean isHasMore() {
        return hasMore;
    }

    public void setHasMore(boolean hasMore) {
        this.hasMore = hasMore;
    }
}
