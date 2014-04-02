package net.webby.plexian.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PlexianUtils {

    private final static Log log = LogFactory.getLog(PlexianUtils.class);
    
    public static Map<String, Set<String>> parseObjectsFieldsExpression(String paramValue) {
        Map<String, Set<String>> docs = new HashMap<String, Set<String>>();
        for (String value : paramValue.split(",")) {
            String[] terms = value.split("\\.");
            if (terms.length == 2) {
                String luceneDoc = StringUtils.trimToNull(terms[0]);
                String luceneField = StringUtils.trimToNull(terms[1]);
                if (luceneDoc != null && luceneField != null) {
                    Set<String> luceneFields = docs.get(luceneDoc);
                    if (luceneFields == null) {
                        luceneFields = new HashSet<String>();
                        docs.put(luceneDoc, luceneFields);
                    }
                    luceneFields.add(luceneField);
                }
                else {
                    log.error("invalid expression " + value + ", some is null Entity.Field");
                }
            }
            else {
                log.error("invalid expression " + value + ", must be like Entity.Field");
            }
        }
        return docs;
    }
    
}
