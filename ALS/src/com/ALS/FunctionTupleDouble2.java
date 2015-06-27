package com.ALS;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2; 

public class FunctionTupleDouble2 implements Function<Tuple2<Double, Double>, Object> {

	@Override
	public Object call(Tuple2<Double, Double> pair ) throws Exception {
		Double err = pair._1() - pair._2();
        return err * err;
	}

}
