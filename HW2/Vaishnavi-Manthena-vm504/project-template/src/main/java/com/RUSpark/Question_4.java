/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.RUSpark;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.*;
import java.lang.Math;

public final class Question_4 {

	public static double getEuclideanDistance(Iterable<Double> p1, Iterable<Double> p2){

		List<Double> p1l = new ArrayList<Double>();
    		for(double i: p1){
			p1l.add(i);
		}
		List<Double> p2l = new ArrayList<Double>();
                for(double i: p2){
                        p2l.add(i);
                }

		double sum = 0.0;

		for(int i = 0; i < p1l.size(); i++){
			sum += Math.pow((p1l.get(i) - p2l.get(i)), 2);
		}		

		return Math.sqrt(sum);


	}

	public static Iterable<Double> helper1(String s){

		ArrayList<Double> entries = new ArrayList<Double>();
		String[] s_entries = s.split(" ");

		for(int i = 0; i < s_entries.length; i++){
			double entry = Double.parseDouble(s_entries[i].strip());
			entries.add(entry);
		}

		return entries;

	}

	public static Tuple2<Iterable<Double>, Iterable<Tuple2<Iterable<Double>, Double>>> helper2(Iterable<Double> dp, List<Iterable<Double>> centroids){
		//This arraylist holds the value list of the output tuple
		ArrayList<Tuple2<Iterable<Double>, Double>> values = new ArrayList<Tuple2<Iterable<Double>, Double>>();

		for(int i = 0; i < centroids.size(); i++){
			//get distance between dp and current centroid
			Iterable<Double> centroid = centroids.get(i);
			double dis = getEuclideanDistance(dp, centroid);
			Tuple2<Iterable<Double>, Double> value = new Tuple2<Iterable<Double>, Double>(centroid, dis);
			values.add(value);
		}

		return new Tuple2<Iterable<Double>, Iterable<Tuple2<Iterable<Double>, Double>>>(dp, values);

	}

	public static Tuple2<Iterable<Double>, Double> helper3(Iterable<Tuple2<Iterable<Double>, Double>> centroidDistances){

		Iterable<Double> result = null;

		double minDis = Double.MAX_VALUE;

		for(Tuple2<Iterable<Double>, Double> pair: centroidDistances){

			if(pair._2() < minDis){
				minDis = pair._2();
				result = pair._1();
			}

		}

		return new Tuple2<Iterable<Double>, Double>(result, minDis);

	}

	public static Iterable<Double> helper4(Iterable<Iterable<Double>> input){

		//convert input to arraylist
		ArrayList<ArrayList<Double>> dps = new ArrayList<ArrayList<Double>>();
		for(Iterable<Double>in: input){
			ArrayList<Double> dp = new ArrayList<Double>();
			for(double e: in){
				dp.add(e);
			}
			dps.add(dp);
		}
		
		double num = dps.size();
		ArrayList<Double> result = new ArrayList<Double>();
		for(int i = 0; i < (dps.get(0)).size(); i++)
			result.add(0.0);

		for(int i = 0; i < num; i++){
			//add this datapoint to result
			ArrayList<Double> dp = dps.get(i);
			for(int j = 0; j < dp.size(); j++)
				result.set(j, result.get(j)+dp.get(j));

		}

		for(int i = 0; i < result.size(); i++)
			result.set(i, result.get(i)/num);

		return result;

	}

  	public static void main(String[] args) throws Exception {

    		if (args.length < 1) {
      			System.err.println("Usage: Question_4 <file>");
      			System.exit(1);
    		}
		
		String DataInputPath = args[0];
		String CentroidsInputPath = args[1];

    		SparkSession spark = SparkSession
      			.builder()
      			.appName("Question_4")
      			.getOrCreate();

		
    		JavaRDD<String> lines = spark.read().textFile(DataInputPath).javaRDD();
		JavaRDD<String> lines2 = spark.read().textFile(CentroidsInputPath).javaRDD();

		//Convert string RDD's into iterable double RDD's
		JavaRDD<Iterable<Double>> dataPoints = lines.map(e->helper1(e));
		//The below would be updated after every Map-Reduce round
		JavaRDD<Iterable<Double>> centroids = lines2.map(e->helper1(e));

		int numIterations = 20;
		double[] phi_values = new double[20]; 

		for(int i = 1; i <= numIterations; i++){
		
			//a single map-reduce job
			List<Iterable<Double>> c = centroids.collect();
			//First for each datapoint we want to measure its distance with all of the centroids
			//Key: DataPoint
			//Value: List of tuples. Each tuple: (centroid, dis btw key and this centroid)
			JavaPairRDD<Iterable<Double>, Iterable<Tuple2<Iterable<Double>, Double>>> dp_c_dists = dataPoints.mapToPair(e -> helper2(e, c));

			//Get the centroid of closest distance
			//key: DataPoint
			//Value: nearest centroid and corresponding distance
			JavaPairRDD<Iterable<Double>, Tuple2<Iterable<Double>, Double>> dp_cc = dp_c_dists.mapToPair(e -> new Tuple2<>(e._1(),helper3(e._2())));

			//We can calculate the phi value for this iteration

			//each row holds the minimum square distance to a centroid for corresponding data point
			JavaRDD<Double> eu_squares = dp_cc.map(e->Math.pow((e._2())._2(), 2));

			//getting the phi_value
			double phi_value = eu_squares.reduce((i1, i2) -> i1 + i2);
			phi_values[i-1] = phi_value;
			//end of phi value calculation

			//exchange key and value
			//key:centroid
			//value:datapoint
			JavaPairRDD<Iterable<Double>, Iterable<Double>> cc_dp = dp_cc.mapToPair(e-> new Tuple2<>((e._2())._1(), e._1()));

			//key: centroid
			//value: list of datapoints assigned to this centroids cluster
			JavaPairRDD<Iterable<Double>, Iterable<Iterable<Double>>> cc_dps = cc_dp.groupByKey();

			//New centroid RDD:
			centroids = cc_dps.map(e -> helper4(e._2()));
		}
		/*
		//testing dp_c_dists
		List<Tuple2<Iterable<Double>, Iterable<Tuple2<Iterable<Double>, Double>>>> output1 = dp_c_dists.collect();
		for(Tuple2<Iterable<Double>, Iterable<Tuple2<Iterable<Double>, Double>>> o: output1){
			
			System.out.println("Point: " + o._1());
			System.out.println("List of distances to centroids: ");
			for(Tuple2<Iterable<Double>, Double> o2: o._2()){
				System.out.print("("+ o2._1() + ": " + o2._2() + ") ");
			}
			System.out.println();
		
		}

		List<Tuple2<Iterable<Double>, Tuple2<Iterable<Double>, Double>>> output12 = dp_cc.collect();
                for(Tuple2<Iterable<Double>, Tuple2<Iterable<Double>, Double>> o: output12){

                        System.out.println(o._1() + ": " + (o._2())._1() + ": " + (o._2())._2());

                }

		System.out.println("phi value: " + phi_value);

		List<Tuple2<Iterable<Double>, Iterable<Double>>> output2 = cc_dp.collect();
		for(Tuple2<Iterable<Double>, Iterable<Double>> o: output2){

			System.out.println(o._1() + ": " + o._2());

		}

		List<Tuple2<Iterable<Double>, Iterable<Iterable<Double>>>> output3 = cc_dps.collect();
		for(Tuple2<Iterable<Double>, Iterable<Iterable<Double>>> o: output3){

			System.out.println("Centroid: " + o._1());
			System.out.println("Datapoints: ");
			for(Iterable<Double> dp: o._2()){
				System.out.println(dp);

			}
			System.out.println();

		}

		System.out.println("New Centroids:");
		List<Iterable<Double>> output4 = centroids.collect();
		for(Iterable<Double> point : output4)
			System.out.println(point);
		*/
		System.out.println(CentroidsInputPath);
		System.out.println("Iteration\tphi");
		for(int i = 0; i < numIterations; i++){
			int iteration = i+1;
			System.out.println(iteration + "\t" + phi_values[i]);
		}
    		spark.stop();
  	}
}
