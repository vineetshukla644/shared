/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flinkdemo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;

import java.io.InputStream;
import java.util.Properties;

public class StreamingJob {

  public static void main(String[] args) throws Exception {

    // set up the streaming execution environment

    StreamExecutionEnvironment env = getExecutionEnv();

    // get application properties

    Properties prop = getEnvironmentProperties();

    // Set topology

    DataStream<String> streamSource =
        env.addSource(getKafkaConsumer(prop)).map(StreamingJob::mapToNewVal);

    FlinkKafkaProducer<String> outputStream = getKafkaProducerStream(prop);

    streamSource.addSink(outputStream);

    streamSource.print();

    // execute program
    env.execute("FlinkDemoJob");
  }

  private static FlinkKafkaProducer<String> getKafkaProducerStream(Properties prop) {
    return new FlinkKafkaProducer<String>(
        prop.getProperty("target.topic"),
        new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
        prop,
        FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
  }

  private static FlinkKafkaConsumer<String> getKafkaConsumer(Properties prop) {
    return new FlinkKafkaConsumer<>(
        prop.getProperty("source.topic"), new SimpleStringSchema(), prop);
  }

  private static String mapToNewVal(String value) {

    return value + "hello";
  }

  private static StreamExecutionEnvironment getExecutionEnv() {

    StreamExecutionEnvironment env = null;

    Properties prop = getEnvironmentProperties();

    if (prop.getProperty("env").equals("local")) {
      env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
    } else {
      env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    return env;
  }

  public static Properties getEnvironmentProperties() {

    Properties prop = new Properties();

    try {

      InputStream input =
          StreamingJob.class.getClassLoader().getResourceAsStream("application.properties");
      // load a properties file
      prop.load(input);

    } catch (Exception ex) {
      ex.printStackTrace();
    }

    return prop;
  }
}
