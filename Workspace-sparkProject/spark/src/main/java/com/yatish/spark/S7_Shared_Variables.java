package com.yatish.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

public class S7_Shared_Variables {

	public static void main(String[] args) {
		
		SparkConf sc = new SparkConf().setAppName("Shared Variables").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		/*
		 * What are Shared Variables?
		 * There are 2 types of Shared Variables,
		 * 1) Broadcast Variables
		 * 2) Accumulators.
		 * 
		 * 
		 * What is Braodcast Variables?
		 * In spark, consider We are performing following operations,
		 * 		1) Do some transformation on 'a','b','c','d'		
		 * 		2) do an action on transformed data				--> Now, The operation and data 'a','b','c','d' are shipped/copied to the worker nodes.
		 * 		3) Do some transformation on 'a','b','c','d'
		 * 		4) Do some action on transformed data.			--> Now, The operation and data 'a','b','c','d' are shipped/copied to the worker nodes again.
		 * So, Broadcast variables, will solve this in-efficiency of copying the data 'a','b','c','d' each and every time the operation is submitted to the worker nodes. Consider if we broadcast,
		 * 'a','b','c','d' then this data will be copied to each and every worker node and it will be cached in each and every worker node. Hence there will be no need to send this data to worker
		 * node whenever the operation is submitted, in our example instead of sending them 2 times it will be only one time data transfer.
		 * 
		 * NOTE:
		 * ----- 
		 * 1) The broadcasted data will be copied to each and every worker node and cached so be careful, if the data we are broadcasting is too big such that it cannot be fitted in a worker node,
		 * 		then there will be out of memory exceptions in the worker nodes so it is always suggested to make sure that worker nodes can handle the broadcasted data.
		 * 2) The broadcast data is always immutable(read-only). no worker node can alter this data.
		 * 
		 * Tell me one usecase, for using braodcast variables?
		 * Consider the ALS algorithm in machine learning,
		 * Once we get the predictions for a user we cannot directly show that predictions to the user because that prediction will be having the product id, so we have to first change the product
		 * ID with the product name/description and then show it to the user. Consider i call a map to replace the id with name as follows,
		 * 			JavaRDD<Rating> rattingRDD = ........
		 * 			rattingRDD.map(rdd -> {
		 * 				Here we are getting each rating entry which has product id. To replace the product id with the product name, we have to load the file having the id to product name map. If
		 * 				we load it here, then the file will be loaded each and every time the function is executed for each 'rdd'. Hence it is very inefficient. So in this kind of situation we can
		 * 				broadcast the product id to product name map so that we can get the details from the broadcasted variable.
		 * 			})
		 * 			
		 * NOTE:
		 * -----
		 * In the Interview, mention that Broadcast is very useful in-case of joins. We can broadcast the smaller map/data participating in the join to increase the efficiency. Rather than explaing
		 * all the above details. If he is still not convinced, then example in depth as explained above.
		 * 
		 */
		
		Map<Integer,String> map = new HashMap<Integer,String>();
		map.put(1, "Mungaru Male");
		Broadcast<Map<Integer,String>> braodcastedVariable = jsc.broadcast(map);
		
		
		
		/*
		 * Accumulators
		 * ------------
		 * Consider the code blow,  we want to count or do some sum or something like that. in below code, we have declared 'count' as global variable and my goal is to add the 'integerRDD' elements.
		 * There is a compile error in map transformation as shown in below code. We cannot access the outside global scope variable unless it is declared as final. if we decalre it as final, then
		 * we cannot add the rdd values to it. This is were accumulators are introduced in spark. Check the next comment section to see how accumulators work.
		 * 
		 */
		Integer count = 0;
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
		JavaRDD<Integer> integerRDD = jsc.parallelize(data);
		integerRDD.foreach(rdd -> {
			count = count + rdd;
		});
		
		/*
		 * Instead of declaring the 'count' as a normal data type, we create special accumulator variable 'LongAccumulator' as shown in the below code. This value willl be accessible and can be 
		 * modifyable by all the worker nodes. There are lot of other built-in accumulators like 'DoubleAccumulator','CollectionAccumulator' etc.
		 * 
		 * NOTE:
		 * -----
		 * We can write custom accumulators as well.
		 */
		LongAccumulator count1 = jsc.sc().longAccumulator();
		JavaRDD<Integer> integerRDD1 = jsc.parallelize(data);
		integerRDD1.foreach(rdd -> {
			count1.add(rdd);
		});
		System.out.println("Accumulator value : " + count1.value());
		

	}

}
