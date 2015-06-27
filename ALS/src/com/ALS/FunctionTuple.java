package com.ALS;


import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2; 
public class FunctionTuple implements Function<Rating, Tuple2<Object, Object>>{
	
	public Tuple2<Object, Object> call(Rating r) {
        return new Tuple2<Object, Object>(r.user(), r.product());
      }
	
	

}
