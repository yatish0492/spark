package com.yatish.streaming;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

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

public class S5_Windowing {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		Map<String,Object> kafkaParams = new HashMap<String,Object>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		Set<String> topics = new HashSet<String>();
		topics.add("myTopic");
		
		/*
		 * In this stream, spark will fetch the inputs for every 5 seconds.
		 */
		JavaInputDStream<ConsumerRecord<String, String>> stream =
				  KafkaUtils.createDirectStream(
				    jssc,
				    LocationStrategies.PreferConsistent(),
				    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
				  );
		
		
		JavaPairDStream<String,String> DataStream1 = stream.mapToPair(a -> {
			return new Tuple2<String,String>(a.key(),a.value());
		});
		
		
		/* 
		 * What is Spark window Streaming?
		 * Consider, streaming source sends data as follows,
		 * 'a' at 8:00:00
		 * 'b' at 8:00:01
		 * 'c' at 8:00:02
		 * 'd' at 8:00:03
		 * 'e' at 8:00:04
		 * 'f' at 8:00:05
		 * 'g' at 8:00:06
		 * 'h' at 8:00:07
		 * 'i' at 8:00:08
		 * 'j' at 8:00:09
		 * 'k' at 8:00:10
		 * 'l' at 8:00:11
		 * 'm' at 8:00:12
		 * 'n' at 8:00:13
		 * 'o' at 8:00:14
		 * 'p' at 8:00:15
		 * 'q' at 8:00:16
		 * 'r' at 8:00:17
		 * 's' at 8:00:18
		 * 't' at 8:00:19
		 * 'u' at 8:00:20
		 * 'v' at 8:00:21
		 * normal spark Streaming
		 * ----------------------
		 * Consider, i use 'foreachRDD' action on the stream. as follows,
		 * 				stream.foreachRDD(rdd -> {....... });
		 * Spark will fetch data with 5 seconds interval as specified while creating the 'javaStreamingContext', it will fetch,
		 * 		'a','b','c','d','e' at 8:00:05, 'foreachRDD' will be executed and 'rdd' value will be 'a','b','c','d','e'. after 5 seconds spark will fetch, 
		 * 		'f','g','h','i','j' at 8:00:10, 'foreachRDD' will be executed and 'rdd' value will be 'f','g','h','i','j'. after 5 seconds spark will fetch, 
		 * 		'k','l','m','n','o' at 8:00:15, 'foreachRDD' will be executed and 'rdd' value will be 'k','l','m','n','o'. after 5 seconds spark will fetch ,
		 * 		'p','q','r','s','t' at 8:00:20, 'foreachRDD' will be executed and 'rdd' value will be 'p','q','r','s','t'. after 5 seconds spark will fetch,
		 * 		'u','v' at 8:00:25. 'foreachRDD' will be executed and 'rdd' value will be 'u','v'
		 * 
		 * window spark Streaming
		 * ----------------------
		 * Consider, the windowsLength is 15 seconds and slide window is 10 seconds.
		 * Consider, i use 'foreachRDD' action on the stream. as follows,
		 * 				stream1.foreachRDD(rdd -> {....... });
		 * Spark will fetch data with 5 seconds interval as specified while creating the 'javaStreamingContext', it will fetch,
		 * 		'a','b','c','d','e' 'e'  +  'f','g','h','i','j' at 8:00:10, 'foreachRDD' will be executed and 'rdd' value will be 'a','b','c','d','e' 'e'  +  'f','g','h','i','j' recent 15 second
		 * 				data. but streaming source has only 10 second data, it will show that only. after 10 seconds spark will fetch,
		 * 		'f','g','h','i','j' + 'k','l','m','n','o' + 'p','q','r','s','t' at 8:00:20, 'foreachRDD' will be executed and 'rdd' value will be f','g','h','i','j' + 'k','l','m','n','o' + 
		 * 				'p','q','r','s','t'. spark considered only last 15 second data as we have mentioned the 'window lenght' as 15 seconds before 8:00:20, hence the data from 8:00:00 to 8:00:05
		 * 				is not considered.
		 * 		'u','v' at 8:00:30. 'foreachRDD' will be executed and 'rdd' value will be 'u','v'
		 * The 'slide window' is nothing but the collection interval/batch interval. the 'slide interval' will override the previously mentioned 'batch interval' provided during 'JavaStreamingContext'
		 * creation only for this stream. if we are calling an action like 'print()' on other normal stream like 'stream.print()', then the spark will be collecting the data at batch interval of
		 * 5 seconds only instead of 10 seconds as the 'slide interval' is applied to only 'stream1' not to 'stream'
		 * 
		 * NOTE:
		 * -----
		 * 1) Always, the 'window length' and the 'slide interval' should be in multiples of whatever the batch interval that is mentioned during 'JavaStreamingContext' creation. like we have give it 
		 * 	  as 5 seconds so 'window length' and 'slide interval' should be of multiples of 5 only. if we give 3,2 etc, then there will be a compile time error.
		 * 
		 * 
		 * What if we give the 'window length' lesser than 'slide interval' like 10 and 30?
		 * In this case there will be no compile time error but spark will ignore the window length and consider it as equal to the 'slide interval' like in this case spark will consider 'window length'
		 * as 30 only. So you have to always give the 'window length' bigger than the 'slide window' otherwise it will be same as just setting batch interval as 30 while creating 'JavaStreamingContext'
		 * instead of using windowing.
		 * 
		 * why do we need windowing?
		 * Consider you want to do some operation on the recently collected batch data combining with previously collected batch/batches data then it is very useful.
		 * eg: consider you are analyzing a log file, you will fetch the log file data every second but you want last 1 hour log to analyze/troubleshoot if any error. you cannot simply set batch interval
		 * 		as 1 hour without using windowing in this case because consider you recieve data from 1PM to 2PM and there is some error at 1:30PM, to analyze that error you need last 1 hour data that 
		 * 		means, the data from 12:30 to 1:30 but you only have only half an hour data from 1PM to 1:30PM, hence windowing is needed.
		 * 
		 * 
		 */
		JavaPairDStream<String,String> stream1 = DataStream1.window(Durations.seconds(5),Durations.seconds(10));
		
		
		/*
		 * There is one more variation of 'window' function without 'sliding window' parameter as shown in below code. in this function it will consider the 'sliding window' value from the 'batch interval'
		 * provided during 'JavaStreamingContext' creation. Like in this case 'sliding window' will be assigned as 5 seconds.
		 * 
		 */
		JavaPairDStream<String,String> stream2 = DataStream1.window(Durations.seconds(30));
		
		/*
		 * If we perform any operation on a stream which is created using 'window' method on an existing stream, then those operations will be executed per each window data not a batch data. In
		 * this case, 'foreachRDD' will be called for each window data not the batch data. i.e. 'rdd' will be assigned with window data not with a batch data.
		 */
		stream1.foreachRDD(rdd -> {
			rdd.foreach(a -> {
				System.out.println(a.toString());
				
			});
		});
		
		
		/*
		 * If we are performing any windowed operations. then it is mandatory to specify the 'checkpointing' for that "javaStreamingContext' as the widow length might be bigger so on system failure
		 * recovery(Fault tolerence), it will be a huge load to fetch so much data after system recovery and if there are lot of transformations done before the action is called then those 
		 * transformations will take a lot of time. and more the time the system is down the more the data you need to process and you need to slide which will make lot more data. Hence it will take
		 * lot of time for the recovery so spark has made it mandatory to checkpoint when using windowed operations. 
		 * 
		 */
		jssc.checkpoint("/Users/yatcs/spark_temp_files");
		stream.countByWindow(Durations.seconds(20), Durations.seconds(10)).print();
		
		stream1.print();
		
		/*
		 * There are lot of window based operations supported by spark using following methods,
		 * 1) .countByWindow(windowDuration, slideDuration)
		 * 2) .groupByKeyAndWindow(windowDuration)
		 * 3) .reduceByKeyAndWindow(reduceFunc, windowDuration)  etc. 
		 */

		jssc.start();
		try {
			jssc.awaitTermination();
		}catch(Exception e) {}
		
		
		

	}

}








