/*
 * Copyright 2022 Atomic Wire Technology Limited.
 */
package com.patricklucas.flink;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.SinkWriter.Context;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchEmitter;
import org.apache.flink.connector.elasticsearch.sink.RequestIndexer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

@Slf4j
public class FlinkGsEsProblem {

  public static void main(String[] args) throws Exception {
    var esEmitter =
        new ElasticsearchEmitter<String>() {
          @Override
          public void emit(String element, Context context, RequestIndexer indexer) {
            var indexRequest = new IndexRequest("my-index").source(element, XContentType.JSON);
            indexer.add(indexRequest);
          }
        };

    var esHost = new HttpHost("localhost", 9200);
    var sink = new Elasticsearch7SinkBuilder<>().setEmitter(esEmitter).setHosts(esHost).build();

    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.fromElements("{\"hello\": \"world\"}").sinkTo(sink);
    env.execute(FlinkGsEsProblem.class.getSimpleName());
  }
}
