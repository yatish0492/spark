package com.yatish.datasets;

import java.util.ArrayList; 
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.yatish.Pojo.Person;

public class S2_DataSet_From_JSON_File_And_Querying_And_Views {

	public static void main(String[] args) {
		
		SparkSession session = SparkSession.builder().master("local").appName("DataSets").getOrCreate();
		Person obj = new Person();
		obj.setName("yatish");
		obj.setAge(27);
		
		/*
		 * NOTE: Always whatever the bean class we provide to 'Encoders' needs to be public class. Hence if we define 'Person' class in this java file itself, then it will throw an error because it 
		 * will be of default type.
		 */
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		
		Dataset<Person> dataSet = session.createDataset(Collections.singletonList(obj),personEncoder);
		dataSet.show();
		
		
		/*
		 * Like how we created a dataframe(Dataset<Row>) from the json file contents, we can create a dataset as well as shown in the below code.
		 * 
		 * NOTE: we have to pass the corresponding encoder while reading the json file to map it to 'Parent' class file.
		 */
		Dataset<Person> personDataSet = session.read().json("src/main/resources/data.json").as(personEncoder);
		personDataSet.show();
		
		
		/*
		 * Consider you want to create a dataset with in-built Object type like Sting,Integer etc In those cases, spark provides in-built encoders, 
		 * like instead of  'Encoder<String> stringEncoder = Encoders.bean(String.class);' we can directly write 'Encoder<String> stringEncoder = Encoders.STRING();'
		 * 
		 */
		Encoder<String> stringEncoder = Encoders.STRING();
		Dataset<String> stringDataSet = session.read().textFile("src/main/resources/data.txt").as(stringEncoder);
		stringDataSet.show();
		
		
		/*
		 * Dataset also performs all operations supported on RDD like 'map' etc.
		 */
		Dataset<Person> mapDoneParentDataSet = personDataSet.map(a -> {
			a.setAge(50);
			return a;
			},personEncoder);
		mapDoneParentDataSet.show();
		
		
		/*
		 * Datasets also support all sort of querying functions supported on dataframes as shown below.
		 * 
		 * NOTE:
		 * -----
		 * Only '.filter' method supports the query to be written using lambda whereas all other methods like 'select','groupBy' etc needs query to be written in using '.col' only.
		 */
		personDataSet.show();
		personDataSet.printSchema();
		personDataSet.select(personDataSet.col("name")).show();
		personDataSet.groupBy(personDataSet.col("name")).count().show();
		
		personDataSet.filter(person -> person.getAge() >25 && person.getAge() < 50).show();
		
		
		/*
		 * Like DataFrames we can create views on datasets as well. Datasets also support both types of views '.createGlobalTempView.' and 'createOrReplaceTempView'
		 */
		personDataSet.select(personDataSet.col("age").gt(25)).createOrReplaceTempView("peopleWithAgeGreaterThan25");
		session.sql("SELECT * FROM peopleWithAgeGreaterThan25").show();
		
		
		
		
	}

}
	

