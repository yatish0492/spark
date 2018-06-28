package com.yatish.DataFrame;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class S5_Generic_Save_Load_Functions {

	public static void main(String[] args) {
		
		SparkSession session1 = SparkSession.builder().master("local").appName("sparkSQL").getOrCreate();
		Dataset<Row> dfFromJSON = session1.read().json("src/main/resources/data.json");
		//Dataset<Row> dfFromParquet = session1.read().load("src/main/resources/data.parquet");
		dfFromJSON.write().save("/Users/yatcs/Downloads/abc/a.parquet");
		//dfFromParquet.show();

	}

}
