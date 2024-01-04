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
package com.cognitree.flume.sink.elasticsearch;

import com.cognitree.flume.sink.elasticsearch.client.BulkProcessorBuilder;
import com.cognitree.flume.sink.elasticsearch.client.ElasticsearchClientBuilder;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.cognitree.flume.sink.elasticsearch.Constants.*;
import static com.cognitree.flume.sink.elasticsearch.Indexer.DEFAULT_INDEXER;
import static com.cognitree.flume.sink.elasticsearch.Serializer.DEFAULT_SERIALIZER;

/**
 * This sink will read the events from a channel and add them to the bulk processor.
 * <p>
 * This sink must be configured with mandatory parameters detailed in
 * {@link Constants}
 */
public class ElasticSearchSink extends AbstractSink implements Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchSink.class);

    private static final int CHECK_CONNECTION_PERIOD = 3000;

    private final AtomicBoolean shouldBackOff = new AtomicBoolean(false);
    private BulkProcessorBuilder bulkProcessorBuilder;
    private ElasticsearchClientBuilder clientBuilder;
    private BulkProcessor bulkProcessor;
    private Indexer indexer;
    private Serializer serializer;
    private RestHighLevelClient client;
    private SinkCounter sinkCounter;
    private int batchSize = 100;


    public RestHighLevelClient getClient() {
        return client;
    }

    @Override
    public void configure(Context context) {
        String[] hosts = getHosts(context);
        if (ArrayUtils.isNotEmpty(hosts)) {
            String clusterName = context.getString(ES_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
            clientBuilder = new ElasticsearchClientBuilder(clusterName, hosts);
            bulkProcessorBuilder = BulkProcessorBuilder.builder(context);
            buildIndexer(context);
            buildSerializer(context);
        } else {
            LOG.error("Could not create Rest client, No host exist");
        }
        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
    }

    @Override
    public Status process() {
        if (shouldBackOff.get()) {
            throw new NoNodeAvailableException("Check whether Elasticsearch is down or not.");
        }
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        txn.begin();
        int total = 0;
        try {
            for (int i = 0; i < batchSize; i++) {
                Event event = channel.take();
                if (event != null) {
                    total++;
                    sink(event);
                }
            }
            if (total == 0) {
                sinkCounter.incrementBatchEmptyCount();
            } else if (total < batchSize) {
                sinkCounter.incrementBatchUnderflowCount();
            } else {
                sinkCounter.incrementBatchCompleteCount();
            }
            txn.commit();
            sinkCounter.addToEventDrainSuccessCount(total);
            return Status.READY;
        } catch (Throwable tx) {
            sinkCounter.incrementEventWriteOrChannelFail(tx);
            try {
                txn.rollback();
            } catch (Exception ex) {
                LOG.error("exception in rollback.", ex);
            }
            LOG.error("transaction rolled back.", tx);
            return Status.BACKOFF;
        } finally {
            txn.close();
        }
    }

    private void sink(Event event) {
        String index = indexer.getIndex(event);
        if (index == null) {
            LOG.debug("Sink event ignored, event is: {}", Util.dump(event));
            return;
        }
        String type = "_doc";
        String id = indexer.getId(event);
        XContentBuilder xContentBuilder = serializer.serialize(event);
        if (xContentBuilder != null) {
            if (id != null && !id.isEmpty()) {
                bulkProcessor.add(new IndexRequest(index, type, id)
                        .source(xContentBuilder));
            } else {
                bulkProcessor.add(new IndexRequest(index, type)
                        .source(xContentBuilder));
            }
        }
    }

    @Override
    public void start() {
        sinkCounter.start();
        try {
            client = clientBuilder.build();
            bulkProcessor = bulkProcessorBuilder.build(this);
            sinkCounter.incrementConnectionCreatedCount();
        } catch (Exception e) {
            LOG.error("Error when starting: {}", e.getMessage(), e);
            sinkCounter.incrementConnectionFailedCount();
            if (client != null) {
                try {
                    client.close();
                } catch (IOException ignored) {
                    //ignore
                } finally {
                    sinkCounter.incrementConnectionClosedCount();
                }
            }
        }
    }

    @Override
    public void stop() {
        try {
            if (bulkProcessor != null) {
                bulkProcessor.close();
            }
            if (client != null) {
                client.close();
            }
        } catch (IOException e) {
            sinkCounter.incrementConnectionFailedCount();
        }
        sinkCounter.incrementConnectionClosedCount();
        sinkCounter.stop();
        super.stop();
    }

    private void buildIndexer(Context context) {
        String indexerClass = DEFAULT_INDEXER;
        if (context.getString(Indexer.PREFIX) != null) {
            indexerClass = context.getString(Indexer.PREFIX);
        }
        this.indexer = Indexer.getInstance(indexerClass);
        if (this.indexer != null) {
            Context indexerContext = new Context(context.getSubProperties(Indexer.PREFIX + "."));
            this.indexer.configure(indexerContext);
        }
    }

    private void buildSerializer(Context context) {
        String serializerClass = DEFAULT_SERIALIZER;
        if (context.getString(Serializer.PREFIX) != null) {
            serializerClass = context.getString(Serializer.PREFIX);
        }
        this.serializer = Serializer.getInstance(serializerClass);
        if (this.serializer != null) {
            Context serializerContext = new Context(context.getSubProperties(Serializer.PREFIX + "."));
            this.serializer.configure(serializerContext);
        }
    }

    private String[] getHosts(Context context) {
        String[] hosts = null;
        if (StringUtils.isNotBlank(context.getString(ES_HOSTS))) {
            hosts = context.getString(ES_HOSTS).split(",");
        }
        return hosts;
    }

    /**
     * Checks for elasticsearch connection
     * Sets shouldBackOff to true if bulkProcessor failed to deliver the request.
     * Resets shouldBackOff to false once the connection to elasticsearch is established.
     */
    public void assertConnection() {
        shouldBackOff.set(true);
        final Timer timer = new Timer();
        final TimerTask task = new TimerTask() {
            @Override
            public void run() {
                try {
                    if (checkConnection()) {
                        shouldBackOff.set(false);
                        timer.cancel();
                        timer.purge();
                    }
                } catch (IOException e) {
                    LOG.error("ping request for elasticsearch failed " + e.getMessage(), e);
                }
            }
        };
        timer.scheduleAtFixedRate(task, 0, CHECK_CONNECTION_PERIOD);
    }

    private boolean checkConnection() throws IOException {
        return client.ping(RequestOptions.DEFAULT);
    }

}
