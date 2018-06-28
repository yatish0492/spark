package com.yatish.spark;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;

public class S3_RDD_External_DataSource_MongoDB {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		SparkSession session1 = SparkSession.builder().master("local").appName("MongoConnector")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection")
				.getOrCreate();
		
		Encoder<student> studentEncoder = Encoders.bean(student.class);
		Dataset<student> dataSet = session1.read().json("src/main/resources/data.json").as(studentEncoder);
		
		Map<String, String> writeOverrides = new HashMap<String, String>();
	    writeOverrides.put("collection", "spark");
	    WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides); 
		MongoSpark.save(dataSet,writeConfig);
		
		/*
		 * Reading complete collection
		 */
		Map<String, String> readOverrides = new HashMap<String, String>();
	    readOverrides.put("collection", "spark");
	    readOverrides.put("readPreference.name", "secondaryPreferred");
	    ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
		//Dataset<student> result = MongoSpark.load(jsc,student.class);
		System.out.println("save done");
		
		session1.stop();
	}

}

class student implements Serializable{
	String name;
	String age;
	
	public student() {
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
