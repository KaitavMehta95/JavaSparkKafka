/**
 * Created by Kaitav Mehta on 2022-04-29
 */


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class SparkKafkaConsumer {
    public static void main(String[] args) throws InterruptedException {

        System.out.println("Spark Streaming started now..");
        SparkConf sparkConf = new SparkConf().setAppName("kafka-sandbox");
        // comment below line after test finished with local
        sparkConf.setMaster("local[*]");

//        sparkConf.set("spark.driver.bindAddress", "127.0.0.1");

        // batchDuration - The time interval at which streaming data will be divided into batches
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9096");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "GROUD_ID_LOCAL"); // todo
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Collections.singletonList("spark-test-local");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );


        JavaPairDStream<String, String> lines = stream.mapToPair(kafkaRecord -> new Tuple2<>(kafkaRecord.key(), kafkaRecord.value()));

        lines.foreachRDD(rdd -> {
            rdd.values().foreachPartition(p -> {
                while (p.hasNext()) {
                    System.out.println("Value of Kafka queue:" + p.next());
                }
            });
        });

        streamingContext.start();
        System.out.println("stream start");
        streamingContext.awaitTermination();
        System.out.println("stream completed");
    }
}
