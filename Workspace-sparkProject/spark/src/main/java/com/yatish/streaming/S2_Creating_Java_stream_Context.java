package com.yatish.streaming;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import scala.Tuple2;

public class S2_Creating_Java_stream_Context {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		
		/*
		 * Like sparkConf we create, we need to create 'JavaStreamingContext' to use spark streaming.
		 * We have to send 2 parameter to 'JavaStreamingContext' constructor as follows,
		 * 1) 'conf' this is the sparkConf we create
		 * 2) 'Duration.seconds(1)' this will specify in what intervals, spark should read the data from kafka. In this case, we are telling spark to read the kafka every one second.
		 * 
		 */
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		Map<String,Object> kafkaParams = new HashMap<String,Object>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		Set<String> topics = new HashSet<String>();
		topics.add("myTopic");
		
		JavaInputDStream<ConsumerRecord<String, String>> stream =
				  KafkaUtils.createDirectStream(
				    jssc,
				    LocationStrategies.PreferConsistent(),
				    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
				  );

		//stream.print();
		
		stream.foreachRDD(rdd -> {
			//OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
			//System.out.println(rdd.collect());
			rdd.foreach(b -> {
				System.out.println(b.toString());
			});
			
		});
		
		
		JavaPairDStream<String,String> DataStream1 = stream.mapToPair(a -> {
			return new Tuple2<String,String>(a.key(),a.value());
		});
		//DataStream1.print();
		
		jssc.start();
		try {
		jssc.awaitTermination();
		}catch(Exception e) {}
		System.out.println("Last -----------");
		
		
//		System.out.println("Stream created" + stream);
//		stream.mapToPair(record -> {
//			System.out.println(record.key());
//			System.out.println(record.value());
//			return new Tuple2<>(record.key(), record.value());
//		});
		
		
		//System.out.println("done with map operation");
		
		
		
	}

}
