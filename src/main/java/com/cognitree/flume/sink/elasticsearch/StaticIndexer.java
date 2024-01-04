package com.cognitree.flume.sink.elasticsearch;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class StaticIndexer implements Indexer {
    private static final String INDEX = "index";
    private static final String DEFAULT_INDEX = "default";

    private String index;

    @Override
    public String getIndex(Event event) {
        return index;
    }

    @Override
    public String getId(Event event) {
        return null;
    }

    @Override
    public void configure(Context context) {
        String index = Util.getContextValue(context, INDEX);
        this.index = StringUtils.isBlank(index) ? DEFAULT_INDEX : index;
        log.info("Static Indexer: index [{}].", this.index);
    }

}
