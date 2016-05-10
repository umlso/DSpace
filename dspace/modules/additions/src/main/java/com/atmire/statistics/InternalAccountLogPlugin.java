package com.atmire.statistics;

import com.atmire.statistics.plugins.SolrLoggerLogPlugin;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.dspace.content.DSpaceObject;
import org.dspace.core.ConfigurationManager;
import org.dspace.core.Context;
import org.dspace.eperson.EPerson;
import org.dspace.eperson.Group;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by: Roeland Dillen (roeland at atmire dot com)
 * Date: 17 Apr 2014
 */
@Component
public class InternalAccountLogPlugin implements SolrLoggerLogPlugin {

    private static Set<Group> internalGroups = null;

    public static Set<Group> getInternalGroups(Context context) throws SQLException {
        if (internalGroups == null) {
            internalGroups = new HashSet<>();

            String[] groupNames = ConfigurationManager.getStringArrayProperty("atmire-cua", "statistics.internal.groups");

            for (String groupName : groupNames) {
                Group group = Group.findByName(context, groupName);
                if (group != null) {
                    internalGroups.add(group);
                }
            }
        }
        return internalGroups;
    }

    /**
     * log4j logger
     */
    private static Logger log = Logger.getLogger(InternalAccountLogPlugin.class);

    @Override
    public void contributeToSolrDoc(DSpaceObject dspaceObject, HttpServletRequest request, EPerson currentUser, SolrInputDocument document) {

        String isInternalKey = "isInternal";
        if (isInternal(currentUser)) {
            if (document.getFieldValue(isInternalKey) != null) {
                document.remove(isInternalKey);
            }
            document.addField(isInternalKey, true);
        }
    }

    protected boolean isInternal(EPerson currentUser) {
        Context context = null;
        try {
            context = new Context();

            Group[] groups = Group.allMemberGroups(context, currentUser);
            Set<Group> internalGroups = getInternalGroups(context);

            for (Group userGroup : groups) {
                for (Group internalGroup : internalGroups) {
                    if (internalGroup.getID() == userGroup.getID()) {
                        return true;
                    }
                }
            }

        } catch (SQLException e) {
            log.error("Error", e);
        } finally {
            if (context != null) {
                context.abort();
            }
        }

        return false;

    }

}
