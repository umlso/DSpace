<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet
        xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
        xmlns:dim="http://www.dspace.org/xmlns/dspace/dim"
        xmlns:s="http://www.etdadmin.com/ns/etdsword"
        xmlns:str="http://exslt.org/strings"
        xmlns:date="http://exslt.org/dates-and-times"
        version="1.1"
        extension-element-prefixes="str date">

    <xsl:output indent="yes" omit-xml-declaration="yes"/>

    <xsl:variable name="smallcase" select="'abcdefghijklmnopqrstuvwxyz'" />
    <xsl:variable name="uppercase" select="'ABCDEFGHIJKLMNOPQRSTUVWXYZ'" />

    <!-- Catch all.  This template will ensure that nothing
         other than explicitly what we want to xwalk will be dealt
         with -->
    <xsl:template match="text()"></xsl:template>

    <!-- match the top level descriptionSet element and kick off the
         template matching process -->
    <xsl:template match="/s:DISS_submission">
        <dim:dim>
            <xsl:if test="./s:DISS_description/s:DISS_advisor/s:DISS_name">
                <xsl:for-each select="./s:DISS_description/s:DISS_advisor/s:DISS_name">
                    <dim:field mdschema="dc" element="contributor" qualifier="advisor">
                        <xsl:value-of select="./s:DISS_surname"/>
                        <xsl:text>,</xsl:text>
                        <xsl:value-of select="./s:DISS_fname"/>
                    </dim:field>
                </xsl:for-each>
            </xsl:if>

            <xsl:if test="./s:DISS_description/s:DISS_dates/s:DISS_comp_date">
                <dim:field mdschema="dc" element="date" qualifier="issued">
                    <xsl:value-of select="./s:DISS_description/s:DISS_dates/s:DISS_comp_date"/>
                </dim:field>
            </xsl:if>

            <xsl:if test="./s:DISS_description/s:DISS_degree">
                <dim:field mdschema="thesis" element="degree" qualifier="level">
                    <xsl:value-of select="./s:DISS_description/s:DISS_degree"/>
                </dim:field>
            </xsl:if>
        </dim:dim>
    </xsl:template>

</xsl:stylesheet>
