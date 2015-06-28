package com.ALS;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Model;
//import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;                  
public class MatrixALSMain {
	
	
	 SparkConf conf = new SparkConf().setAppName("Collaborative Filtering Example");
	 JavaSparkContext sc = new JavaSparkContext(conf);
	 
	 String path = "data/mllib/als/test.data";
	 JavaRDD<String> data = sc.textFile(path);
	 
	 JavaRDD<Rating> ratings = data.map(new FunctionRating());
	 
	
	// Build the recommendation model using ALS
	    int rank = 10;
	    int numIterations = 20;
	    MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01); 
	   
	     
	JavaRDD<Tuple2<Object, Object>> userProducts = ratings.map(new FunctionTuple());
	

/*	
	 JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
		      model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(
		        new Function<Rating,Tuple2<Tuple2<Integer, Integer>, Double>>() {
		          public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
		            return new Tuple2<Tuple2<Integer, Integer>, Double>(
		              new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
		          }
		        }
		    ));
	
	 
	 JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
		      model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(new FunctionTupleDouble()));	
*/
	
	 RDD<Tuple2<Object, Object>> userProductsInRDD = JavaRDD.toRDD(userProducts);
	 RDD<Rating> pR = model.predict(userProductsInRDD);
	 int j = model.hashCode();
	 
	// model.save(sc.sc(), "");
	  
    	 	
	 JavaRDD<Rating> pRinRDD = pR.toJavaRDD();
	 JavaRDD< Tuple2<Tuple2<Integer, Integer>, Double>> pRinJavaRDD = pRinRDD.map(new FunctionTupleDouble());
	 JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(pRinJavaRDD);
	 
	 
	 
	 /*
	 JavaRDD<Tuple2<Double, Double>> ratesAndPreds = 
		      JavaPairRDD.fromJavaRDD(ratings.map(
		         new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
		          public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
		            return new Tuple2<Tuple2<Integer, Integer>, Double>(
		              new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
		          } 
		    	  
		        } 
		    		 
		    )).join(predictions).values();
	 
		    double MSE = JavaDoubleRDD.fromRDD(ratesAndPreds.map(
		      new Function<Tuple2<Double, Double>, Object>() {
		        public Object call(Tuple2<Double, Double> pair) {
		          Double err = pair._1() - pair._2();
		          return err * err;
		        }
		      }
		    ).rdd()).mean();

	 JavaRDD<Tuple2<Double, Double>> ratesAndPreds = 
		      JavaPairRDD.fromJavaRDD(ratings.map(new FunctionTupleDouble() )).join(predictions).values();
     double MSE = JavaDoubleRDD.fromRDD(ratesAndPreds.map(new FunctionTupleDouble2()).rdd()).mean(); 		
	*/

	 JavaRDD< Tuple2<Tuple2<Integer, Integer>, Double>> r = ratings.map(new FunctionTupleDouble());
	 JavaPairRDD<Tuple2<Integer, Integer>, Double> k = JavaPairRDD.fromJavaRDD(r); 
	 JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> kp = k.join(predictions);
	 JavaRDD<Tuple2<Double, Double>> ratesAndPreds = kp.values();
	 
	 
	 
	 JavaRDD<Object> jRDD = ratesAndPreds.map(new FunctionTupleDouble2());
	 RDD<Object> rdd = JavaRDD.toRDD(jRDD); 
	 JavaDoubleRDD jDRDD = JavaDoubleRDD.fromRDD(rdd);
	 Double MSE = jDRDD.mean();
	 
	 SparkContext sc1 = sc.sc();
	 String str = "myModelPath";
	 

	 // Save and load model
	// model.save(sc.sc(), "myModelPath");
	 //MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(sc.sc(), "myModelPath");

}
