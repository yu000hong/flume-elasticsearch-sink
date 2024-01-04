/*
 * Copyright 2017 Cognitree Technologies
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.cognitree.flume.sink.elasticsearch.client;

import com.cognitree.flume.sink.elasticsearch.ElasticSearchSink;
import com.cognitree.flume.sink.elasticsearch.Util;
import org.apache.flume.Context;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;

/**
 * This class creates  an instance of the {@link BulkProcessor}
 * Set the configuration for the BulkProcessor through {@link Context} object
 */
public class BulkProcessorBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(BulkProcessorBuilder.class);

    private String bulkProcessorName;

    private ByteSizeValue bulkSize;

    private Integer bulkActions;

    private Integer concurrentRequest;

    private TimeValue flushIntervalTime;

    private String backoffPolicyTimeInterval;

    private Integer backoffPolicyRetries;

    private ElasticSearchSink elasticSearchSink;

    private BulkProcessorBuilder() {

    }

    public static BulkProcessorBuilder builder(Context context) {
        BulkProcessorBuilder builder = new BulkProcessorBuilder();
        builder.bulkActions = context.getInteger(ES_BULK_ACTIONS,
                DEFAULT_ES_BULK_ACTIONS);
        builder.bulkProcessorName = context.getString(ES_BULK_PROCESSOR_NAME,
                DEFAULT_ES_BULK_PROCESSOR_NAME);
        builder.bulkSize = Util.getByteSizeValue(context.getInteger(ES_BULK_SIZE),
                context.getString(ES_BULK_SIZE_UNIT));
        builder.concurrentRequest = context.getInteger(ES_CONCURRENT_REQUEST,
                DEFAULT_ES_CONCURRENT_REQUEST);
        builder.flushIntervalTime = Util.getTimeValue(context.getString(ES_FLUSH_INTERVAL_TIME),
                DEFAULT_ES_FLUSH_INTERVAL_TIME);
        builder.backoffPolicyTimeInterval = context.getString(ES_BACKOFF_POLICY_TIME_INTERVAL,
                DEFAULT_ES_BACKOFF_POLICY_START_DELAY);
        builder.backoffPolicyRetries = context.getInteger(ES_BACKOFF_POLICY_RETRIES,
                DEFAULT_ES_BACKOFF_POLICY_RETRIES);
        return builder;
    }

    public BulkProcessor build(ElasticSearchSink elasticSearchSink) {
        this.elasticSearchSink = elasticSearchSink;
        RestHighLevelClient client = elasticSearchSink.getClient();
        LOG.trace("Bulk processor name: [{}]  bulkActions: [{}], bulkSize: [{}], flush interval time: [{}]," +
                        " concurrent Request: [{}], backoffPolicyTimeInterval: [{}], backoffPolicyRetries: [{}] ",
                bulkProcessorName, bulkActions, bulkSize, flushIntervalTime,
                concurrentRequest, backoffPolicyTimeInterval, backoffPolicyRetries);
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
                (request, bulkListener) -> client
                        .bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
        return BulkProcessor.builder(bulkConsumer, getListener())
                .setBulkActions(bulkActions)
                .setBulkSize(bulkSize)
                .setFlushInterval(flushIntervalTime)
                .setConcurrentRequests(concurrentRequest)
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(
                        Util.getTimeValue(backoffPolicyTimeInterval,
                                DEFAULT_ES_BACKOFF_POLICY_START_DELAY),
                        backoffPolicyRetries))
                .build();
    }

    private BulkProcessor.Listener getListener() {
        return new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                LOG.trace("Bulk Execution [" + executionId + "]\n" +
                        "No of actions " + request.numberOfActions());
            }
            @Override
            public void afterBulk(long executionId,
                                  BulkRequest request,
                                  BulkResponse response) {
                LOG.trace("Bulk execution completed [" + executionId + "]\n" +
                        "Took (ms): " + response.getTook().getMillis() + "\n" +
                        "Failures: " + response.hasFailures() + "\n" +
                        "Failures Message: " + response.buildFailureMessage() + "\n" +
                        "Count: " + response.getItems().length);
            }
            @Override
            public void afterBulk(long executionId,
                                  BulkRequest request,
                                  Throwable failure) {
                elasticSearchSink.assertConnection();
                LOG.error("Unable to send request to elasticsearch.", failure);
            }
        };
    }

}
