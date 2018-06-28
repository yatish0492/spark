package com.yatish.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class S1_create_spark_context {

	public static void main(String[] args) {
		/*
		 * We need to create a spark context object, later on we can convert this spark context 
		 * based on the language we are using like java or python or scala 
		 * 
		 * We give a name here for the application, which will applear on the cluster UI. here it is - 'sparkStart'
		 * 
		 * Then we have to give the URL of the Spark server(master) where it is running.so that the 
		 * spark context gets initialized with actual cluster details from the URL into the program.
		 * 
		 * Types of Masters
		 * ----------------
		 * 1) local
		 * 			Run Spark locally with one worker thread (i.e. no parallelism at all).
		 * 2) local[K]
		 * 			Run Spark locally with K worker threads (ideally, set this to the number of cores on your machine).
		 * 3) local[K,F]
		 * 			Run Spark locally with K worker threads and F maxFailures.
		 * 			what is maxFailures?
		 * 			'maxFailures' means, how many times spark should attempt retry on a failed task before giving up on it.
		 * 4) local[*]
		 * 			Run Spark locally with as many worker threads as logical cores on your machine.
		 * 5) local[*,F]
		 * 			Run Spark locally with as many worker threads as logical cores on your machine and F maxFailures.
		 * 6) spark://HOST:PORT
		 * 			Connect to the given Spark standalone cluster master. The port must be whichever one your master is configured to use, which is 7077 by default.
		 * 7) mesos://HOST:PORT
		 * 			Connect to the given Mesos cluster. The port must be whichever one your is configured to use, which is 5050 by default. Or, for a Mesos cluster using ZooKeeper, 
		 * 			use mesos://zk://.... To submit with --deploy-mode cluster, the HOST:PORT should be configured to connect to the MesosClusterDispatcher. yarn	Connect to a YARN cluster 
					in client or cluster mode depending on the value of --deploy-mode. The cluster location will be found based on the HADOOP_CONF_DIR or YARN_CONF_DIR variable.
		   8) yarn
		   			Connect to a YARN cluster in client or cluster mode depending on the value of --deploy-mode. The cluster location will be found based on the HADOOP_CONF_DIR or YARN_CONF_DIR 
		   			variable.

		 */
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		
		
		
		/*
		 * In above line, we created the sparkConfiguration object. but since we want to use that spark context
		 * in java. there is one more object which helps us use spark context in java through java APIs
		 * 
		 * So we are creating one more object called javasparkcontext using the sparkconf object
		 */
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		
		/*
		 * Once we are done executing the things. we need to close the spark context.
		 */
		sc.close();

	}

}
