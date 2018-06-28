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
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class S4_Storing_Offset {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("sparkStreaming").setMaster("local");
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
	 * Why do we need to store the offset?
	 * A streaming application must operate 24/7 and hence must be resilient to failures unrelated to the application logic (e.g., system failures, JVM crashes, etc.). 
	 * Consider a spark streaming application is have already processed 50 GB of data published by kafka and due to some reason there is a system failure and in 1 hour the engineers will bring back
	 * the system up. In that 1 hour, kafka would have streamed 10 GB of data, once the nodes comes up,
	 * 		if "auto.offset.reset" is set to 'latest' then spark application will start processing the data streamed from the time it is brought back up, so the 10 GB of data streamed in last 1 hour 
	 * 			will not be processed.
	 * 		if "auto.offset.reset" is set to 'earliest' then the spark application will start processing the data from beginning which will also process the 10 GB of data streamed during 1 hour of
	 * 			system failure but, it will also processes the 50 GB of data which was already processed by spark application before failure which is not good.
	 * So, basically we want the spark application to start processing from the 10 GB of data which was streamed during 1 hour of system failure. So if we would have stored the offset before the
	 * system failure happened, then once it came back we would have read the stored offset and started processing from he same point where it had stopped.
	 * That is why we need to store the offset.
	 * 
	 * 
	 * How will be come to know when to store, how do we know when the system will fail?
	 * We need not manually store the offset, spark will take care of it. Spark will automatically save the offset continuously based on some interval. probably batch interval.
	 * 
	 * How to we store the offset?
	 * There are 2 ways as follow,
	 * 1) Checkpointing
	 * 2) Kafka Itself
	 * 3) Your own data store
	 * 
	 * 
	 * What is checkpointing?
	 * Using checkpointing, spark will not only store the offset information, it will store additional information as well to recover from the system failure.
	 * 
	 * What data does checkpointing store?
	 * Checkpointing will store 2 types of data,
	 * 1) Metadata checkpointing
	 *    ----------------------
	 *    This data is very useful to recover, if there is a node failure which was running the driver of streaming application. This contains following informatoin,
	 *    1) Configuration - The configuration that was used to create the streaming application.
	 *    2) DStream operations - The set of DStream operations that define the streaming application.
	 *    3) Incomplete batches - Batches whose jobs are queued but have not completed yet.
	 * 2) Data Checkpointing
	 *    ------------------
	 *    This will save the generated RDDs. This is very useful in case of stateful trasformations that combine data across multiple batches like window operations.
	 *    n such transformations, the generated RDDs depend on RDDs of previous batches, which causes the length of the dependency chain to keep increasing with time. 
	 *    To avoid such unbounded increases in recovery time (proportional to dependency chain), intermediate RDDs of stateful transformations are periodically saved.
	 *    
	 * 
	 * Where will the data be stored in case of checkpointing?
	 * User have to specify a fault tolerent storage system like HDFS directory. in that the checkpointed information will be stored.
	 * 
	 * How does storing offset using 'Kafka Itself' work?
	 * Kafka has an offset commit API that stores offsets in a special Kafka topic. It will only store the offset but not RDDs like checkpointing. It will also periodically save the offset like
	 * checkpointing,It will be auto enabled but we specify 'enable.auto.commit' in kafka params right, that is for this only. it is a 'Kafka Itself' property, it it is set to 'true', then spark 
	 * will commit the  offset periodically kafka, if it is set to 'false', then spark will not commit the offset periodically, if we want we need to commit offset manually to 'Kafka itself' then 
	 * we can manually using 'commitAysnc' method.
	 * 
	 * 
	 * How to enable checkpointing?
	 * just by setting a location(directory) using 'streamingContext.checkpoint(checkpointDirectory)'. we can set the directory location to local directory or an hdfs location as well.
	 * 
	 * 
	 */
	
	/*
	 * So, simply by specifying the 'streamingContext.checkpoint(checkpointDirectory)' will recover from system failure?
	 * NO!!!!
	 * There is the catch, when we specify 'streamingContext.checkpoint(checkpointDirectory)', spark will start checkpointing/storing the metadata and RDDs to the specified directory but when the 
	 * system recovers, the spark application will be started like how it was started for the first time. so we have to explicitly program such that it will check for checkpointed file and recover
	 * from it as follows in the below code
	 */
		
		JavaStreamingContextFactory contextFactory = new JavaStreamingContextFactory() {
			  @Override public JavaStreamingContext create() {
			    JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(5));  // new context									
			    jssc.checkpoint("/Users/yatcs/spark_temp_files");                       // set checkpoint directory
			    return jssc;
			  }
			};
			
		// Get JavaStreamingContext from checkpoint data or create a new one
		JavaStreamingContext context = JavaStreamingContext.getOrCreate("/Users/yatcs/spark_temp_files", contextFactory);

		//After this you can use the above JavaStreaming context 'context' and do streaming operation etc.
		
	}
}
