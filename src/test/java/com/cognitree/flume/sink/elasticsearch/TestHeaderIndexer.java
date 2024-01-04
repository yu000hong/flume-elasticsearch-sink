///*
// * Copyright 2017 Cognitree Technologies
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// * or implied. See the License for the specific language governing
// * permissions and limitations under the License.
// */
//package com.cognitree.flume.sink.elasticsearch;
//
//import org.apache.flume.Context;
//import org.apache.flume.Event;
//import org.apache.flume.event.SimpleEvent;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import static com.cognitree.flume.sink.elasticsearch.Constants.*;
//import static org.junit.Assert.assertEquals;
//
//public class TestHeaderIndexer {
//
//    private HeaderIndexer headerIndexBuilder;
//
//    private String index = "es-index";
//
//    private String type = "es-type";
//
//    private String id = "es-id";
//
//    @Before
//    public void init() {
//        headerIndexBuilder = new HeaderIndexer();
//    }
//
//    /**
//     * tests header based index, type and id
//     */
//    @Test
//    public void testHeaderIndex() {
//        Event event = new SimpleEvent();
//        Map<String, String> headers = new HashMap<>();
//        headers.put(INDEX, index);
//        headers.put(TYPE, type);
//        headers.put(ID, id);
//        event.setHeaders(headers);
//        assertEquals(index, headerIndexBuilder.getIndex(event));
//        assertEquals(id, headerIndexBuilder.getId(event));
//    }
//
//    /**
//     * tests configuration based index and type
//     */
//    @Test
//    public void testConfigurationIndex() {
//        Event event = new SimpleEvent();
//        Context context = new Context();
//        context.put(ES_INDEX, index);
//        context.put(ES_TYPE, type);
//        headerIndexBuilder.configure(context);
//        assertEquals(index, headerIndexBuilder.getIndex(event));
//    }
//
//    /**
//     * tests Default index and type
//     */
//    @Test
//    public void testDefaultIndex() {
//        Event event = new SimpleEvent();
//        assertEquals(DEFAULT_ES_INDEX, headerIndexBuilder.getIndex(event));
//    }
//}
