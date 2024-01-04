package com.cognitree.flume.sink.elasticsearch;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.http.client.utils.DateUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TemplateIndexer implements Indexer {
    private static final String INDEX_TEMPLATE = "index.template";
    private static final String ID_TEMPLATE = "id.template";

    private String indexTemplate;
    private String idTemplate;

    @Override
    public String getIndex(Event event) {
        Map<String, Object> params = new HashMap<>(event.getHeaders());
        appendDateParams(params);
        return Util.template(indexTemplate, params);
    }

    @Override
    public String getId(Event event) {
        if(!StringUtils.isBlank(idTemplate)){
            Map<String, Object> params = new HashMap<>(event.getHeaders());
            appendDateParams(params);
            return Util.template(idTemplate, params);
        }
        return null;
    }

    @Override
    public void configure(Context context) {
        this.indexTemplate = Util.getContextValue(context, INDEX_TEMPLATE);
        this.idTemplate = Util.getContextValue(context, ID_TEMPLATE);
    }


    private void appendDateParams(Map<String, Object> params) {
        Date now = new Date();
        params.put("Y", DateUtils.formatDate(now, "yyyy"));
        params.put("M", DateUtils.formatDate(now, "MM"));
        params.put("D", DateUtils.formatDate(now, "dd"));
        params.put("DATE", DateUtils.formatDate(now, "yyyy-MM-dd"));
    }

}
