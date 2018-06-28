package com.yatish.spark.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class S3_collect {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("spark://10.232.12.82:7077");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		JavaRDD<Integer> rddData1 =	rddData.map(a -> {
			System.out.println("lambda called");
			if(a>5) 
				return a;
			else
				return -1;
		});
		
		/*
		 * Once the RDD is created, if I want the RDD elements to display, how can i do that?
		 * Spark provides a method called '.collect()'. This method will return the element present in the RDD in the form of List.
		 * In this case, 'rddData1.collect()' will return the elements in form of 'List<Integer>'
		 * 
		 * IMPORTANT NOTE
		 * --------------
		 * '.collect()' is not a 'transformation'. it is an 'action'.
		 * The transformations will not be executed unless an action is performed on the respective RDD. In this case, The lambda will be executed, only when java executes the below statement which
		 * contains the action --> 'rddData1.collect()'.
		 * If we comment out the below code, then the lambda will not be executed on execution of this program and 'rddData1' will be empty.
		 * 
		 * 
		 * NOTE: '.collect()' will return all the elements of the RDD in form of array. In background it will bring all the elements(RDDs) present in different name nodes to the master/data node.
		 * 			Hence, there are possibilities of out of memory exception occurring on call of collect() as there might be very huge set of data distributed as RDDs in name nodes, the master/data
		 * 			node might not have that much capacity to hold that huge amount of data.
		 * 
		 * 
		 * So tell me one usecase, where we can use '.collect()'?
		 * In Unit Testing, we will call collect on RDD to compare both expected and actual results are same or not.
		 * 
		 * 
		 */
		System.out.println(rddData1.collect());
		
		
		/*
		 * 'collect()' vs 'no collect()'
		 * -----------------------------
		 * no collect()
		 * ---------
		 * 	rddData.foreach(a -> {
				System.out.println(a);
			});
		   
		   collect()
		   ------------
		   	System.out.println(rddData1.collect());
		   	
		   	
		   	if you execute the 'no collect()' code, you will see the elements printed in corresponding name nodes on which the corresponding RDDs are sitting, it will not be printed on the master
		   	node. consider you have 3 name nodes running in US and your master node is in india, in this case it will print the in the nodes present in US, not in the node present in india.
		   	
		   	if you execute the 'collect()' code, spark will bring all the RDDs to the master node and it will be printed in the master node.
		 */
		sc.stop();
		
	}

}
