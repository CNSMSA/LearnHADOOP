package com.ALS;


import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.Rating;  
public class FunctionRating implements Function<String, Rating>{
	@Override
	 public Rating call(String s) {
         String[] sarray = s.split(",");
         return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]), 
                           Double.parseDouble(sarray[2]));
       }

	
	
	

}

