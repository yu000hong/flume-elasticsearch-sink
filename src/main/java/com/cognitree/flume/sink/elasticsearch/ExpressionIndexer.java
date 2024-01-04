package com.cognitree.flume.sink.elasticsearch;

import lombok.extern.slf4j.Slf4j;
import org.apache.flume.Context;
import org.apache.flume.Event;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.cognitree.flume.sink.elasticsearch.Constants.COMMA;

@Slf4j
public class ExpressionIndexer implements Indexer {
    private static final String INDEX_HEADER = "index.expression";
    private static final String INDEX_INCLUDE = "index.include";
    private static final String INDEX_EXCLUDE = "index.exclude";
    private static final String DEFAULT_INDEX = "default";
    private static final String ID_HEADER = "id.expression";

    private String indexHeader;
    private String idHeader;
    private Set<String> includeIndexes;
    private Set<String> excludeIndexes;

    @Override
    public String getIndex(Event event) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getId(Event event) {
        if (idHeader == null) {
            return null;
        }
        Map<String, String> headers = event.getHeaders();
        return headers.get(idHeader);
    }

    @Override
    public void configure(Context context) {
        this.indexHeader = Util.getContextValue(context, INDEX_HEADER);
        this.idHeader = Util.getContextValue(context, ID_HEADER);
        log.info("Header Indexer: index [{}], id [{}]",
                this.indexHeader, this.idHeader);
        String includeIndexesValue = Util.getContextValue(context, INDEX_INCLUDE);
        if (includeIndexesValue != null) {
            includeIndexes = new HashSet<>();
            includeIndexes.addAll(Arrays.asList(includeIndexesValue.split(COMMA)));
            return; //include和exclude是互斥的
        }
        String excludeIndexesValue = Util.getContextValue(context, INDEX_EXCLUDE);
        if (excludeIndexesValue != null) {
            excludeIndexes = new HashSet<>();
            excludeIndexes.addAll(Arrays.asList(excludeIndexesValue.split(COMMA)));
        }
    }

}
