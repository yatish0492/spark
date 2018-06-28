package com.yatish.DataFrame;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.yatish.Pojo.Person;

public class S3_DataFrame_From_RDD {

	public static void main(String[] args) {
		
		SparkSession session = SparkSession.builder().master("local").appName("DataSets").getOrCreate();
		
		/*
		 * You cannot have 2 sparkContexts in a same application. like here we have 'SparkSession' created and if we try to create a 'SparkConf' using following code,
		 *  		SparkConf conf = new SparkConf().setAppName("DataSets").setMaster("local");
		 *  		JavaSparkContext sc = new JavaSparkContext(conf);
		 *  The above code will give error as we are creating 2 spark contexts, 1 was during session builder. Hence instead of creating one more spark context using above code, we have to use the
		 *  previously created spark context as shown in below code.
		 */
		JavaSparkContext sc = new JavaSparkContext(session.sparkContext());
		JavaRDD<String> stringRDD = sc.textFile("src/main/resources/data.txt");
		
		List<StructField> fields = new ArrayList<StructField>();
		//'createStructField' accepts 3 arguments '(String nameOfField,DataType dataType,boolean canBeNullOrNot)'
		StructField messageCOlumnField = DataTypes.createStructField("Messages",DataTypes.StringType,true);
		fields.add(messageCOlumnField);
		StructType schema = DataTypes.createStructType(fields);
		JavaRDD<Row> rowRDD = stringRDD.map(a -> {
			return RowFactory.create(a);
		});
		Dataset<Row> messageDataFrame = session.createDataFrame(rowRDD, schema);
		messageDataFrame.show();
		
		
		/*
		 * Converting a RDD of Person type into the DataFrame. As we have 'Person' class itself, there is no need for 'schema' instead we can pass 'Person.class' itself
		 */
		Person obj1 = new Person();
		obj1.setName("Yatish");
		obj1.setAge(27);
		Person obj2 = new Person();
		obj2.setName("Gagan");
		obj2.setAge(29);
		List<Person> personList = new ArrayList<Person>();
		personList.add(obj1);
		personList.add(obj2);
		JavaRDD<Person> personRDD = sc.parallelize(personList);
		Dataset<Row> personDataFrame = session.createDataFrame(personRDD,Person.class);
		personDataFrame.show();
		
		
	}
	
}
