package com.yatish.spark.MachineLearning;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

public class S3_1_ALS {

	public static void main(String[] args) {
		
		SparkConf sc = new SparkConf().setAppName("ALS").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		
		// Reading the input data which contains the (userId,productId,Rating)
		JavaRDD<String> data = jsc.textFile("src/main/resources/data3.txt");
		
		//There is a 'Rating' type used by ALS to perform prediction. So we are converting the data read from the text file into 'Rating' RDD.
		JavaRDD<Rating> rattingRDD = data.map(line -> {
			String[] rattingElements = line.split(",");
			return new Rating(Integer.parseInt(rattingElements[0]),Integer.parseInt(rattingElements[1]),Integer.parseInt(rattingElements[2]));
		});
		
		// ALS.train(JavaRDD<Rating> arg1, int rank, int numOfIterations, double lambda);
		/*
		 * What is rank we are passing?
		 * Consider, we are predicting the rating of a movie for users. A user will have many fields, like age,sex,location,language etc. so If ALS considers all the fields of the user and
		 * predict the movie rating for him, then it will take hell lot of time but your predictions will be more accurate. sometimes we need the predictions to be calculated fast so we can tell
		 * ALS to consider only few things like ignore age and location to predict the rating, in this case calculation will be fast but accuracy might by less, here 'rank' is 2. i.e. if we
		 * specify rank as '3' then it will ignore 3 factors for predicting like age,sex,location. 
		 * We can only arrive at how many factors to ignore for rating by only experimenting like we can set it to 5-10 and then see how long it takes to predict and then increase it based on 
		 * how much time we can tolerate for this computation. 
		 * Official definition - "rank refers the presumed latent or hidden factors"
		 * Default value is - 10
		 * 
		 */
		MatrixFactorizationModel model = ALS.train(rattingRDD.rdd(), 10, 5, 0.01);
		
		
		JavaRDD<Tuple2<Object,Object>> userAndProductMapRDD = rattingRDD.map(rating -> {
			return new Tuple2<Object,Object>(rating.user(),rating.product());
		});
		RDD<Rating> modelFromPredictRDD = model.predict(userAndProductMapRDD.rdd());
		JavaRDD<Rating> modelFromPredictJavaRDD = modelFromPredictRDD.toJavaRDD();
		JavaPairRDD<Tuple2<Integer,Integer>,Double> userProductRatingPairMapRDDFromPrediction = modelFromPredictJavaRDD.mapToPair(rating -> {
			return new Tuple2<Tuple2<Integer,Integer>,Double>(new Tuple2<Integer,Integer>(rating.user(),rating.product()),rating.rating());
		});
		
		
		
		JavaPairRDD<Tuple2<Integer,Integer>,Double> userProductRatingPairMapRDD = rattingRDD.mapToPair(rating -> {
			return new Tuple2<Tuple2<Integer,Integer>,Double>(new Tuple2<Integer,Integer>(rating.user(),rating.product()),rating.rating());
		});
		
		JavaRDD<Tuple2<Double, Double>> ratesAndPreds = userProductRatingPairMapRDD.join(userProductRatingPairMapRDDFromPrediction).values(); 
		
		List<Tuple2<Double,Double>> a = ratesAndPreds.collect();
		 for(Tuple2<Double,Double> each : a) {
			 System.out.println("each --> " + each);
		 }
		 
		JavaRDD<Object> mseRDD = ratesAndPreds.map(ratePred -> {
			Double err = ratePred._1() - ratePred._2();
			return err*err;
		});
		JavaDoubleRDD mseDoubleRDD = JavaDoubleRDD.fromRDD(mseRDD.rdd());
		double MSE = mseDoubleRDD.mean();
		System.out.println("MSE ----------> " + MSE);
		ratesAndPreds.count();
		
		System.out.println(ratesAndPreds.take(5).toString());
		
		//model.save(jsc.sc(), "/Users/yatcs/Downloads/a");
		
		/*
		 * We have got the recommended products, which has the id of the product hence we can use that id and show the product details.
		 * 
		 */
		Rating[] recommendations = model.recommendProducts(1, 5);		// 1st argument specify the user id and then 2nd argument species the number of recommendation to be returned out of 'n'.
		System.out.println(recommendations[0] );
		//System.out.println(model.predict(1,4));
		System.out.println("done");
		
	}

}
