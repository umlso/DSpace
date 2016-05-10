/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.app.xmlui.utils;


import org.dspace.authority.AuthorityValue;
import org.dspace.authority.AuthorityValueFinder;
import org.dspace.core.Context;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities that are needed in XSL transformations.
 *
 * @author Art Lowel (art dot lowel at atmire dot com)
 */
public class XSLUtils {
    /* BEGIN Atmire change */
    private static final String REGEX = "/[^/,.]*/\\.\\./";
    private static final Pattern PATTERN = Pattern.compile(REGEX);

    public static String collapseUrl(String url) {
        url = url.replaceAll("/\\./", "/");
        Matcher m = PATTERN.matcher(url);
        while (m.find()) {
            url = url.replaceAll(REGEX, "/");
            m = PATTERN.matcher(url);
        }
        return url;
    }
    /* END Atmire change */

    /*
     * Cuts off the string at the space nearest to the targetLength if there is one within
     * maxDeviation chars from the targetLength, or at the targetLength if no such space is
     * found
     */
    public static String shortenString(String string, int targetLength, int maxDeviation) {
        targetLength = Math.abs(targetLength);
        maxDeviation = Math.abs(maxDeviation);
        if (string == null || string.length() <= targetLength + maxDeviation)
        {
            return string;
        }


        int currentDeviation = 0;
        while (currentDeviation <= maxDeviation) {
            try {
                if (string.charAt(targetLength) == ' ')
                {
                    return string.substring(0, targetLength) + " ...";
                }
                if (string.charAt(targetLength + currentDeviation) == ' ')
                {
                    return string.substring(0, targetLength + currentDeviation) + " ...";
                }
                if (string.charAt(targetLength - currentDeviation) == ' ')
                {
                    return string.substring(0, targetLength - currentDeviation) + " ...";
                }
            } catch (Exception e) {
                //just in case
            }

            currentDeviation++;
        }

        return string.substring(0, targetLength) + " ...";

    }

	public static String getDiscoveryFilterDisplay(String display) throws SQLException {
		Context ctx = new Context();

		if (display != null) {
			if (display.length() == 36 && display.contains("-")) {
				AuthorityValue authorityValue = new AuthorityValueFinder().findByUID(ctx, display);
				if (authorityValue != null) {
					display = authorityValue.getValue();
				}
			}
		}

		ctx.complete();

		return display;
	}
}
