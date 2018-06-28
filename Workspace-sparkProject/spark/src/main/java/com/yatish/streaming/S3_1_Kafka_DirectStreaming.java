package com.yatish.streaming;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
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

public class S3_1_Kafka_DirectStreaming {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		/*
		 * How does Spark Streaming works?
		 * We know that in normal spark streaming, the data will be fetched at the specified interval specified while creating 'JavaStreamingContext' in this program we have configured the interval
		 * as 'Durations.seconds(1)'. hence the data will be fetched from the streaming source for every 1 seconds. consider the streaming source is sending 'x' amount of data between 8:00:00 to 
		 * 8:00:01 seconds and sends 'y' amount of data between 8:00:01 to 8:00:02 and so on, spark will fetch 'x' amount of data at 8:00:05 and then it waits for 1 seconds and then fetches 'y'
		 * amount of data at 8:00:02. 
		 * batch --> the 'x' and 'y' amount of data that is fetched are called batches of data. 'x' amount of data is called one batch and other 'y' amount of data is called one batch.
		 * blocks --> Each batch contains blocks. block will be just chucks of batch data.
		 * each 'batch' data will be converted to 'RDD' and blocks of that batch data will act as partitions of the 'RDD'.
		 * batch interval --> The collection interval is called as batch interval.
		 */
		
		//we keep all the kafka properties for connecting to kafka in a map. The map should be of type '<String,Object>' only.
		Map<String,Object> kafkaParams = new HashMap<String,Object>();
		// we specify the IP address and port of the kafka broker from which we want to stream the data. We can have more than one brokers together like as follows,
		// kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092", .....);
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		//we specify deserialization class for deserializing the key. 'key' recieved from the stream will be the topic name.
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		//we specify deserialization class for deserializing the value. 'value' recieved from the stream will be the actual data sent.
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		//We specify the id for the group of consumers or stream. You need to give different group id for each stream.
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		/*
		 * In kafka, A number is assigned as the messages arrives in a sequential manner. Eg: first message will be assigned 0, the next message will be assigned 1. this nunber is called 'offset'.
		 * By Default, when the stream is created, the initial offset(0th) or the current offset will be considered to start streaming of data, if it does not exist any more on the server 
		 * (e.g. because that data has been deleted).
		 * We can specify, the recovery offset in this case with the values as follows,
		 * 		earliest: automatically reset the offset to the earliest offset, That means consider if publisher has already published following data 'a','b','c' before starting this stream, then 
		 * 				once we start the stream, the stream will fetch the data from beginning that means, 'a','b','c' will be fetched from the stream and action will be called on it. 'a','b','c' 
		 * 				will be considered as a batch data, how much ever big the data is already published before starting the stram, that will be fetched in one batch.
				latest: automatically reset the offset to the latest offset. basically it will set the offset to the latest arrived data to broker. so that before starting the stream, if there is any
						data published to the topic, that data will not be considered and next whatever the data is published after the stream is created will be fetched by the stream.
				none: throw exception to the consumer if no previous offset is found for the consumer's group
				anything else: throw exception to the consumer. 
		 */
		kafkaParams.put("auto.offset.reset", "earliest");
		//If true the consumer's offset will be periodically committed in the background.
		kafkaParams.put("enable.auto.commit", false);
		// we need to put the topics from which we have to fetch the streaming data in the. 
		Set<String> topics = new HashSet<String>();
		topics.add("myTopic");
		
		
		/*
		 * We use the 'kafkaUtils' package provided by the spark-kafka package to create data stream using 'createDirectStream' function.
		 * we send the following parameters to the 'createDirectStream'  as follows,
		 * 		'jssc' --> JavaStreamingContext, this will have all information about streaming source, i.e. kafka
		 * 		'LocationStrategies' --> This will specify how the streamed data should be cached in the cluster.
		 * 								'LocationStrategies.PreferConsistent' --> This will distribute partitions evenly across available executors in the name/worker node.
		 * 								'LocationStrategies.PreferBrokers' --> If your executors/name/worker node are same hosts as your Kafka brokers,which will prefer to cache partitions on the 
		 * 																	  Kafka leader for that partition.
		 * 								NOTE
		 * 								----
		 * 								The maximum cache capacity for storing the streamed data is set to 64 by default in each executors/name/worker node. you can change this setting via 
		 * 								spark.streaming.kafka.consumer.cache.maxCapacity
		 * 
		 * 		'ConsumerStrategies' --> We need to specify the topics to which we are subscribing for the data to be streamed form the source. There are lot of ways to specify the topics and one 
		 * 								among them is 'ConsumerStrategies', it has got one advantage among other which is it will obtain properly configured consumers even after restart from 
		 * 								checkpoint.
		 * 								
		 * 								'ConsumerStrategies' also supports one more method called '.subscribePattern(pattern,kafkaParams)', here the pattern will be 'Pattern pattern = 
		 * 								Pattern.compile("my.*")'. Hence using '.subscribe' we have to pass the list of topics to subscribe, whereas using 'subscribePattern' you can specify a matching
		 * 								pattern instead of a list.
		 * 
		 * 
		 * 							
		 */
		JavaInputDStream<ConsumerRecord<String, String>> stream =
				  KafkaUtils.createDirectStream(
				    jssc,
				    LocationStrategies.PreferConsistent(),
				    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
				  );
		
		
		/*
		 * Spark streaming will always recieve the data in form of 'ConsumerRecord' type only. 'ConsumerRecord' class does not implement 'serializable' hence cannot directly print them like 
		 * 'stream.print()' it will give 'non serialized' exception. Hence it is better to transform the stream of type 'ConsumerRecord<String,String>' to other types like 
		 * 'Tuple2<String,String>' using some transformation as shown in below code.
		 * 
		 */
		JavaPairDStream<String,String> DataStream1 = stream.mapToPair(a -> {
			return new Tuple2<String,String>(a.key(),a.value());
		});
		
		
		/*
		 * Does the above transformation to create 'DataStream1' be executed now?
		 * No!!!!
		 * We know that spark works as lazy execution. The same applies to the streams as well. Until an action is called on the data stream, non of its transformations will be done. but there is
		 * one more catch here, in case of RDD/DataFrames, the compilation of the program will go through fine even if we have not defined any action on them, But in case of streams, if there is no
		 * action defined in the program then during, compile time itself, it will thrown an error 'java.lang.IllegalArgumentException: requirement failed: No output operations registered, so 
		 * nothing to execute'.
		 * 
		 * NOTE:
		 * -----
		 * it is mandatory to define action on the stream.
		 * 
		 * What are the actions supported on streams?
		 * 	print()
			foreachRDD(func)
			saveAsObjectFiles(prefix, [suffix])
			saveAsTextFiles(prefix, [suffix])
			saveAsHadoopFiles(prefix, [suffix])
			
		 *  These actions will be called once right, then how come we can handle the continuous stream of data?
		 *  In case of streams, the action will be called for every data retrival/batch interval configured during initialization of 'JavaStreamingContext', in this program, it is 1 second interval.
		 *  Hence consider if the program starts at 8:00:00, then the  spark will fetch the data from kafka at 8:00:00 and the action will be called on that data. then at 8:00:01, the spark will
		 *  collect the data streamed by kafka from 8:00:00 to 8:00:01 and call the action on this data. then at 8:00:02, the spark will collect the data streamed by kafka from 8:00:01 to 8:00:02 
		 *  and call the action on this data and so on.
		 */
		
		
		/*
		 * 'print()' action example. This will print the batch data recieved from kafka.
		 * 
		 * NOTE:
		 * -----
		 * if we call 'print()' on 'stream' instead of 'DataStream1' like 'stream.print()', then spark will throw the following exception,
		 * 'org.apache.spark.SparkException: Job aborted due to stage failure: Task 0.0 in stage 0.0 (TID 0) had a not serializable result: org.apache.kafka.clients.consumer.ConsumerRecord' because
		 * stream is fetches the data in form of 'ConsumerRecord' type, which is not serialized class hence. it will throw an error so only we have transformed the 'stream' to 'DataStream1' which
		 * will convert 'ConsumerRecod' type data into 'Tuple2' type which is serialized.
		 */
		DataStream1.print();
		
		
		/*
		 * 'foreachRDD' action example. This will act as same as that of 'foreach' function of RDD/DataFrame, where foreach will be executed for each partition of the RDD/DataFrame but in case of
		 * streams, the 'foreachRDD' will be executed for each batch data will be acting as a RDD.
		 */
		stream.foreachRDD(rdd -> {
				System.out.println(rdd);
		});
		
		
		/*
		 * 'saveAsHadoopFiles' action example. This will save the streamed data into HDFS. For each batch interval one file will be created and the batch data will be dumped into it.
		 * 
		 * In the below example, it will save the file under 'hdfs://localhost:8020/user/spark/stream/' with name as the time in milliseconds and the suffix will be '.txt'
		 * eg: The file created will be - hdfs://localhost:8020/user/spark/stream/943438934934.txt
		 * 
		 * NOTE :
		 * -------
		 * This might throw a runtime exception if it is not able to connect to HDFS
		 */
		DataStream1.saveAsHadoopFiles("hdfs://localhost:8020/user/spark/stream/","txt");
		
		
		/*
		 * Like RDD/DataFrame, Don't think that the action/streaming will be started by just defining the action. In case of streams we have to explicitly call '.start()' method on the 
		 * 'JavaStreamingContext' to start streaming.
		 */
		jssc.start();
		
		
		/*
		 * Don't think that by just calling '.start()', your job will be done because, '.start()' method will start the consumer thread and stop, you have to make it a deamon thread to run in
		 * background like garbage collector so that it will be alive in background and collect the data from kafka continuously. to make it a deamon thread, you have to call '.awaitTermination()'
		 * method on the 'JavaStreamingContext'.
		 * 
		 * NOTE:
		 * -----
		 * 1) you can stop the deamon thread by calling '.stop()' on the 'JavaStreamingContext'
		 * 2) we can call '.awaitTerminationOrTimeout(long timeout)' also. This is same as '.awaitTermination()' but this will be terminated automatically once the timeout expires if the 
		 * 		consumer/daemon thread is not manually stopped by '.stop()'
		 * 
		 */
		try {
			jssc.awaitTermination();
		}catch(Exception e) {}
		
		/*
		 * !!!!!!! IMPORTANT !!!!!!!!!
		 * 1) We cannot create more than one 'JavaStreamingContext' like how we cannot have more than one 'JavaSparkContext'. There can be only one 'JavaSparkContext' and 'JavaStreamingContext' per 
		 * 	  JVM.
		 * 2) We can have more than one stream(DStream) but only one 'JavaStreamingContext' is allowed.
		 */

	}

}
