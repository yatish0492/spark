package com.yatish.DataFrame;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class S1_SparkSession {

	public static void main(String[] args) {
		
		/*
		 * Why do we first of all create session/SparkSession?
		 * Spark Session is particular to Spark-SQL, if we want to use DataFrame/DataSet, then we need session otherwise if we are just working using RDDs, then there is no need of session.
		 * 
		 * We have to create a spark session first like how we create session in JDBC or Hibernate.
		 * 
		 * NOTE: Even though we are not creating the sparkConf here while creating spark session, SparkSession will internally create a sparkConf if doesn't exist and if exists, then it will use it 
		 *		internally.
		 * 
		 * We have created a spark session but we have not specified any details of the DB it needs to connect, so to where it is connecting and maintaining the session?
		 * Actually, In spark SQL, you can get the data from files also instead of DB. so while creating the session we don't give any details about DB, we will provide it later, not during creation
		 * of session.
		 * 
		 * 
		 * NOTE:
		 * -----
		 * We have to call '.getOrCreate()' at the end.
		 * 
		 */
		SparkSession session = SparkSession.builder().master("local").appName("SparkStreaming").getOrCreate();
		
		
		/*
		 * We can create as many sessions as we want. in this same file, we have created 2 sessions 'session' and 'session1'
		 */
		SparkSession session1 = SparkSession.builder().master("local").appName("SparkStreaming1").getOrCreate();
		
		
		/*
		 * You cannot have 2 sparkContexts in a same application. like here we have 'SparkSession' created and if we try to create a 'SparkConf' using following code,
		 *  		SparkConf conf = new SparkConf().setAppName("DataSets").setMaster("local");
		 *  		JavaSparkContext sc = new JavaSparkContext(conf);
		 *  The above code will give error as we are creating 2 spark contexts, 1 was during session builder. Hence instead of creating one more spark context using above code, we have to use the
		 *  previously created spark context as shown in below code.
		 */

	}

}
