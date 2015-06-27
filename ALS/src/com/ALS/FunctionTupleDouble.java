package com.ALS;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2; 
public class FunctionTupleDouble implements Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>{
	
	public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
        return new Tuple2<Tuple2<Integer, Integer>, Double>(
          new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
      }
	

}
