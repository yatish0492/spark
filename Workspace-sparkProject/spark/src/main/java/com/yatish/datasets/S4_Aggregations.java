package com.yatish.datasets;

import java.util.Collections;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.yatish.Pojo.Person;

public class S4_Aggregations {

	/*
	 * There are no changes, same as how we do in dataframes
	 */
	public static void main(String[] args) {

		SparkSession session = SparkSession.builder().master("local").appName("DataSets").getOrCreate();
		Person obj = new Person();
		obj.setName("yatish");
		obj.setAge(27);
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> dataSet = session.createDataset(Collections.singletonList(obj),personEncoder);
		dataSet.createOrReplaceTempView("dataSet_view");
		
		session.sql("SELECT avg(age) as Average_Age FROM dataSet_view").show();
		
		/*
		 * We can create custom user-defined aggregate functions as well. refer to - https://spark.apache.org/docs/latest/sql-programming-guide.html#aggregations
		 */
	}

}
