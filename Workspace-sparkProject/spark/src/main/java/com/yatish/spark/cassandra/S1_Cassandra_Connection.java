package com.yatish.spark.cassandra;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;

public class S1_Cassandra_Connection {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf(true)
			    .setMaster("local")
			    .setAppName("DatastaxTests")
			    .set("spark.executor.memory", "1g")
			    .set("spark.cassandra.connection.host", "localhost")
			    .set("spark.cassandra.connection.port", "9042");
			   // .set("spark.cassandra.connection.rpc.port", "9160");
		
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		SparkContextJavaFunctions functions = CassandraJavaUtil.javaFunctions(jsc);
		
		CassandraTableScanJavaRDD<CassandraRow> rdd = functions.cassandraTable("roadtrips", "roadtrip");
		System.out.println(rdd.count());
		System.out.println("done");
		
//		SparkSession spark = SparkSession
//	              .builder()
//	              .appName("SparkCassandraApp")
//	              .config("spark.cassandra.connection.host", "localhost")
//	              .config("spark.cassandra.connection.port", "9042")
//	              .master("local[2]")
//	              .getOrCreate();
//		
//		CassandraConnector connector = CassandraConnector.apply(spark.sparkContext().conf());
//		Session session = connector.openSession();
//		session.execute("CREATE TABLE mykeyspace.mytable(id UUID PRIMARY KEY, username TEXT, email TEXT)");

	}

}
