/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.app.util;

import java.sql.SQLException;
import java.util.*;

import org.dspace.content.Collection;
import org.dspace.content.Community;
import org.dspace.core.ConfigurationManager;

/**
 * Utility class for lists of collections.
 */

public class CollectionDropDown {

    /**
     * Get full path starting from a top-level community via subcommunities down to a collection.
     * The full path will not be truncated.
     * 
     * @param col 
     *            Get full path for this collection
     * @return Full path to the collection
     */
    public static String collectionPath(Collection col) throws SQLException
    {
        return CollectionDropDown.collectionPath(col, 0);
    }
    
    /**
     * Get full path starting from a top-level community via subcommunities down to a collection.
     * The full cat will be truncated to the specified number of characters and prepended with an ellipsis.
     * 
     * @param col 
     *            Get full path for this collection
     * @param maxchars 
     *            Truncate the full path to maxchar characters. 0 means do not truncate.
     * @return Full path to the collection (truncated)
     */
    public static String collectionPath(Collection col, int maxchars) throws SQLException
    {
        String separator = ConfigurationManager.getProperty("subcommunity.separator");
        if (separator == null)
        {
            separator = " > ";
        }
        Community[] getCom = null;
        StringBuffer name = new StringBuffer("");
        getCom = col.getCommunities(); // all communities containing given collection

        name.append(col.getMetadata("name"));

        return name.toString();
    }

	/**
	 * Annotates an array of collections with their respective full paths (@see #collectionPath() method in this class).
	 * @param collections An array of collections to annotate with their hierarchical paths.
	 *                       The array and all its entries must be non-null.
	 * @return A sorted array of collection path entries (essentially collection/path pairs).
	 * @throws SQLException In case there are problems annotating a collection with its path.
	 */
	public static CollectionPathEntry[] annotateWithPaths(Collection[] collections) throws SQLException
	{
		CollectionPathEntry[] result = new CollectionPathEntry[collections.length];
		for (int i = 0; i < collections.length; i++)
		{
			Collection collection = collections[i];
			CollectionPathEntry entry = new CollectionPathEntry(collection, collectionPath(collection));
			result[i] = entry;
		}
		Arrays.sort(result);
		return result;
	}

	/**
	 * A helper class to hold (collection, full path) pairs. Instances of the helper class are sortable:
	 * two instances will be compared first on their full path and if those are equal,
	 * the comparison will fall back to comparing collection IDs.
	 */
	public static class CollectionPathEntry implements Comparable<CollectionPathEntry>
	{
		public Collection collection;
		public String path;

		public CollectionPathEntry(Collection collection, String path)
		{
			this.collection = collection;
			this.path = path;
		}

		@Override
		public int compareTo(CollectionPathEntry other)
		{
			if (!this.path.equals(other.path))
			{
				return this.path.compareTo(other.path);
			}
			return Integer.compare(this.collection.getID(), other.collection.getID());
		}

		@Override
		public boolean equals(Object o)
		{
			return o != null && o instanceof CollectionPathEntry && this.compareTo((CollectionPathEntry) o) == 0;
		}

		@Override
		public int hashCode()
		{
			return Objects.hash(path, collection.getID());
		}
	}
}
