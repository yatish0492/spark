package com.yatish.streaming;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import scala.Tuple2;

public class S3_2_Kafka_createRDD {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("sparkStreaming").setMaster("local");
		/*
		 * Like how we cannot have more than a 'JavaSparkContext' in a JVM, we cannot have both 'JavaSparkContext' and 'JavaStreamingContext'. we can either have only one of them. 
		 * In the below code, I have commented out 'JavaStreamingContext' if we uncomment and run the program there will be a compile time error.
		 */
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		//JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(5));
		Map<String,Object> kafkaParams = new HashMap<String,Object>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		Set<String> topics = new HashSet<String>();
		topics.add("myTopic");
		
		OffsetRange[] offsets = {
			// create(Topic_Name, Partition_Number, Inclusive_starting_Offset, Exclusive_Ending_Offset)
			OffsetRange.create("myTopic",0,0,10),
			OffsetRange.create("myTopic",1,0,10)
		};
		
		
		/*
		 * We had 'KafkaUtils.createDstream right then why we need to create this 'KafkaUtils.createRDD' which creates a RDD?
		 * Actually it works the same way as that of Dstream, like 'JavaRDD<ConsumerRecord<String, String>>' is equivalent of 'JavaInputDStream<ConsumerRecord<String, String>>'. This also
		 * will stream data like data streams but one difference is that, in Dstreams they will be streaming the batch data continuously. whereas here we specify a range using offsets. 
		 * it will only stream the data within that range. In this code, we have specified initial offset as '0' and ending offset as '10' so spark will only fetch the offset within 
		 * this range from the partition '0', whereas in Dstreams we cannot specify a custom offset range to fetch data only within that range.
		 * Example:
		 * --------
		 * Consider kafka is streaming following data --> 1,2,3,4,5,6,7,8,9,10 to a topic and we have specified only 2 partitions for this topic, then following will be the partitions data
		 * 					partition 0 --> 1,3,5,7,9
		 * 					partition 1 --> 2,4,6,8,10
		 * 		kafka will put one element to one partition and next element to next partition till it covers all the partitions and then round robin fashion.
		 * Dstream will fetch data in following fashion irrespective of partitions --> 1,2,3,4,5,6,7,8,9,10
		 * createRDD() will fetch in follwing fashion consider offset as 'OffsetRange.create("myTopic",0,2,5)' --> 5,7
		 * 			consider if you specify a ending offset bigger like 'OffsetRange.create("myTopic",0,2,10)' --> Then it will fetch 5,7 and wait. whenever the data 
		 * 																										 is published to that partition, that data will fetched like Dstream
		 * 																										 but only until it reaches the ending offset '10' after that even if 
		 * 																										 data is published to the partition, no operations will be performed on that. 
		 *
		 * 
		 * NOTE: 
		 * -----
		 * 1) we have to specify from which partition we have to fetch the data within the offset.
		 * 2) Even though we have specified 2 offset in the 'offsets' variable, it is considering only the first array element like in this code, it is getting the data form on '0'th partition
		 * 		not from both '0'th and '1'st partition.
		 * 3) The data published to a topic will be distributed across the partitions in the round robbin fashion.
		 * 
		 */
		JavaRDD<ConsumerRecord<String, String>> rdd = KafkaUtils.createRDD(sparkContext, kafkaParams, offsets, LocationStrategies.PreferConsistent());
		System.out.println("before tranformation");
		JavaPairRDD<String,String> pairRDD = rdd.mapToPair(partition -> {
			System.out.println("key -->" + partition.key());
			System.out.println("Value -->" + partition.value());
			return new Tuple2<String,String>(partition.key(),partition.value());
		});
		System.out.println("after tranformation");
		System.out.println(pairRDD.count());
		
		
	}

}
