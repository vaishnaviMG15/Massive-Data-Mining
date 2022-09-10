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

public final class Question_1 {

	

	private static ArrayList<Tuple2<Integer, Iterable<Integer>>> helper1(String s){
		//try{
		//System.out.println("helper1: input string: " + s);	
		ArrayList<Tuple2<Integer, Iterable<Integer>>> result = new ArrayList<Tuple2<Integer, Iterable<Integer>>>();

		//Creating the iterable integer part
		//This is the same for all elements of the result
		ArrayList<Integer> user_and_friends = new ArrayList<Integer>();

		String user_str = s.split("\\t")[0].strip();
		//System.out.println("helper1: user_str: " + user_str);	
		int user = Integer.parseInt(user_str);

		user_and_friends.add(user);

		//Now you need to add all the friends of this user to this array.
		
		String[] friends = (s.split("\\t")[1]).split(",");
		
		for(int i = 0; i < friends.length; i++){

			//convert this friend to an integer and add to user_and_friends
			int friend = Integer.parseInt(friends[i].strip());
			user_and_friends.add(friend);

		}

		//Iterable<Integer> user_and_friends_it = Arrays.asList(user_and_friends);

		for(int i = 0; i < friends.length; i++){

                        //convert this friend to an integer and create a new result element
                        int friend = Integer.parseInt(friends[i].strip());
                        //Tuple2<Integer, Iterable<Integer>> element = new Tuple2<>(friend, user_and_friends_it);
			Tuple2<Integer, Iterable<Integer>> element = new Tuple2<>(friend, user_and_friends);
			result.add(element);

                }

		return result;
		//}catch(Exception e){

		//	System.out.println(e);
		//	System.out.println(s);
		//	return null;

		//}

	}	

	public static ArrayList<Tuple2<Tuple2<Integer, Integer>, Integer>> helper2(Iterable<Iterable<Integer>> input){


		ArrayList<Tuple2<Tuple2<Integer, Integer>, Integer>> result = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Integer>>();

		ArrayList<ArrayList<Integer>> in = new ArrayList<ArrayList<Integer>>();

		for(Iterable<Integer> l : input){

			ArrayList<Integer> l_al = new ArrayList<Integer>();

			for(int i: l){
				l_al.add(i);
			}
			
			in.add(l_al);

		}

		//At this point in is the array list version of the inut.

		for(int i = 0; i < in.size(); i++){
			for(int j = i+1; j < in.size(); j++){

				//considering the potential mutual users i and j
				//to be mutual users, i can't be in j's list
				boolean areMutual = true;
				for(int k : in.get(j)){

					if(k == (in.get(i)).get(0)){
						areMutual = false;
						break;
					}

				}

				if(areMutual){

					Tuple2<Integer, Integer> pair1 = new Tuple2<Integer, Integer>((in.get(i)).get(0), (in.get(j)).get(0));
					Tuple2<Integer, Integer> pair2 = new Tuple2<Integer, Integer>((in.get(j)).get(0), (in.get(i)).get(0));

					Tuple2<Tuple2<Integer, Integer>, Integer> e1 = new Tuple2<Tuple2<Integer, Integer>, Integer>(pair1, 1);
					Tuple2<Tuple2<Integer, Integer>, Integer> e2 = new Tuple2<Tuple2<Integer, Integer>, Integer>(pair2, 1);

					result.add(e1);
					result.add(e2);

				}

			}
		}	
		
		return result;		

	}

	public static Tuple2<Integer, Tuple2<Integer, Integer>> helper3(Tuple2<Tuple2<Integer, Integer>,Integer> input){

		Tuple2<Integer, Integer> pair = input._1();
		int useri = pair._1();
		int userj = pair._2();
		int sim = input._2();

		Tuple2<Integer, Integer> pairNew = new Tuple2<Integer, Integer>(userj, sim);
	        Tuple2<Integer, Tuple2<Integer, Integer>> result = new Tuple2<Integer, Tuple2<Integer, Integer>>(useri, pairNew);
       		return result;	       

	}	

	public static ArrayList<Tuple2<Integer, Integer>> helper4(Iterable<Tuple2<Integer, Integer>> input){
		
		//[(2,1) (8,2) (7, 2)]
		//[(7,2) (8,2) (2,1)] 
		System.out.println("===========");

		//To store the input as an arraylist of points
		ArrayList<Point> in = new ArrayList<Point>();

		for(Tuple2<Integer, Integer> e: input){

			in.add(new Point(e._1(), e._2()));

		}

		for(Point x: in){
			System.out.print(x + ", ");
		}
		System.out.println();

		//Now we need to sort 'in'
		Collections.sort(in);
		
		ArrayList<Tuple2<Integer, Integer>> result = new ArrayList<Tuple2<Integer, Integer>>();

		for(Point p: in){
                        System.out.print(p + ", ");
			Tuple2<Integer, Integer> e = new Tuple2<Integer, Integer>(p.x, p.y);
			result.add(e);
                }
                System.out.println();

		return result;
		
		
	}

	public static Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>> helper5(Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>> input){
		
		//[(2,1) (8,2) (7, 2)]
		//[(7,2) (8,2) (2,1)] 
		//System.out.println("===========");

		//To store the input as an arraylist of points
		ArrayList<Point> in = new ArrayList<Point>();

		for(Tuple2<Integer, Integer> e: input._2()){

			in.add(new Point(e._1(), e._2()));

		}

		//for(Point x: in){
		//	System.out.print(x + ", ");
		//}
		//System.out.println();

		//Now we need to sort 'in'
		Collections.sort(in);
		
		ArrayList<Tuple2<Integer, Integer>> result = new ArrayList<Tuple2<Integer, Integer>>();

		for(Point p: in){
                        //System.out.print(p + ", ");
			Tuple2<Integer, Integer> e = new Tuple2<Integer, Integer>(p.x, p.y);
			result.add(e);
                }
                //System.out.println();

		return new Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>>(input._1(), result);
		
		
	}

	public static Tuple2<Integer, String> helper6(Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>> input){

		int user  = input._1();

		String result = "\t";

		//int num = (input._2()).size();

		int toRecord = 10;

		//if(num <10){

			//toRecord = num;
		//}

		//Get the top 'toRecord userj's from input._2()'

		//Converting input._2() to an iterable
		//ArrayList<Tuple2<Integer, Integer>> input2L = new ArrayList<Tuple2<Integer, Integer>>();

		int i = 1; 
		for(Tuple2<Integer, Integer> pair: input._2()){

			int userj = pair._1();

                        if(i == 1){
                                result += userj;
                        }else{
                                result += "," + userj;
                        }
			
			i++;
			if(i > toRecord){
				break;
			}

		}
		/*
		for(int i = 0; i < toRecord; i++){

			Tuple2<Integer, Integer> pair = (input._2()).get(i);
			int userj = pair._1();
			if(i == 0){
				result += userj;
			}else{
				result += "," + userj;
			}	

		}
		*/

		return new Tuple2<Integer, String>(user, result);

	}

  	public static void main(String[] args) throws Exception {

    		if (args.length < 1) {
      			System.err.println("Usage: Question_1 <file>");
      			System.exit(1);
    		}
		
		String InputPath = args[0];

    		SparkSession spark = SparkSession
      			.builder()
      			.appName("Question_1")
      			.getOrCreate();

		//Input Format: <UserID><tab><Friends>
    		JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();

		JavaRDD<String> lines_to_process = lines.filter(s -> (s.split("\\t")).length > 1);

		JavaRDD<String> lines_to_hold = lines.filter(s -> (s.split("\\t")).length <= 1);

		//Key: friend id
		//Value: (user id, friend1id, friend2id, .....,)
		JavaPairRDD<Integer, Iterable<Integer>> friend_to_userInfo = lines_to_process.flatMapToPair(e -> (helper1(e)).iterator());

		//Do reduce by key to the above
		JavaPairRDD<Integer, Iterable<Iterable<Integer>>> friend_to_usersInfo = friend_to_userInfo.groupByKey();

		//We don't really need the key anymore. We know that in the value list, users are potentially mutual
		JavaRDD<Iterable<Iterable<Integer>>> pot_mutual_users = friend_to_usersInfo.map(e->e._2());
		
		//key: (useri, userj)
		//value: 1

		JavaPairRDD<Tuple2<Integer, Integer>,Integer> mutual_users = pot_mutual_users.flatMapToPair(e -> (helper2(e)).iterator());	
		
		//key: (useri, userj)
		//value: total similarity

		JavaPairRDD<Tuple2<Integer, Integer>,Integer> mutualUsers_Sim = mutual_users.reduceByKey((i1, i2) -> i1 + i2);	

		//Key: User i
		//Value: (user j, similarity)

		JavaPairRDD<Integer, Tuple2<Integer, Integer>> user_to_mutUsers = mutualUsers_Sim.mapToPair(e -> helper3(e));	

		//Key: User i
		//Value: List of (userj, similarity) pairs
		
		JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> user_to_mutUsers_List = user_to_mutUsers.groupByKey();

		//Same as above but ordering the List in 1)descending order of similarity 2)ascending order of userj

		//JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> user_to_mutUsers_List_Sorted = user_to_mutUsers_List.mapToPair(e -> 
		//		new Tuple2<>(e._1(),e._2().sortBy((e._2())._2())));

		//System.out.println("At Last Transformation==========================================\n\n\n");
		//JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> user_to_mutUsers_List_Sorted = user_to_mutUsers_List.mapToPair(e -> 
                //              new Tuple2<>(e._1(),helper4(e._2())));
		
		JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> user_to_mutUsers_List_Sorted = user_to_mutUsers_List.mapToPair(e ->
                              helper5(e));

		//System.out.println("Done With Last Transformation==========================================\n\n\n");
		
		
		//Key: useri
		//Value: comma seperated list of top 10 mutual user j's (as a string)

		JavaPairRDD<Integer, String> result = user_to_mutUsers_List_Sorted.mapToPair(e -> helper6(e)); 
		
		//Same as above but keys are sorted

		JavaPairRDD<Integer, String> resultFinal = result.sortByKey();	
		
		//Output
		/*
    		List<Tuple2<Integer, Iterable<Integer>>> output = friend_to_userInfo.collect();
    		for (Tuple2<?,?> tuple : output) {
      			System.out.println(tuple._1() + " " + tuple._2());
    		}
		*/
		/*
		List<Iterable<Iterable<Integer>>> output = pot_mutual_users.collect();
                for (Iterable<Iterable<Integer>> o : output) {
                        System.out.println(o);
                }
		*/
		
		/*	
		List<Tuple2<Tuple2<Integer, Integer>,Integer>> output = mutual_users.collect();
                for (Tuple2<Tuple2<Integer, Integer>,Integer> o : output) {
			Tuple2<Integer, Integer> pair = o._1();
			int sim = o._2();
                        System.out.println("(" + pair._1()+ ","+ pair._2() + ") :" + sim);
                }

		*/
	
		/*	
		List<Tuple2<Tuple2<Integer, Integer>,Integer>> output = mutualUsers_Sim.collect();
                for (Tuple2<Tuple2<Integer, Integer>,Integer> o : output) {
                        Tuple2<Integer, Integer> pair = o._1();
                        int sim = o._2();
                        System.out.println("(" + pair._1()+ ","+ pair._2() + ") :" + sim);
                }
		*/
		/*	
			
		List<Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>>> output = user_to_mutUsers_List.collect();
                for (Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>> o : output) {
			int user = o._1();
			System.out.print(user + ": ");

                        for (Tuple2<Integer, Integer> pair : o._2()){
                        	int userj = pair._1();
				int sim = pair._2();
                        	System.out.print("(" +userj+ ","+ sim + ") ");
                	}
			System.out.println();
			
		}
		
		System.out.println("After Sorting:");
		List<Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>>> output2 = user_to_mutUsers_List_Sorted.collect();
                for (Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>> o : output2) {
                        int user = o._1();
                        System.out.print(user + ": ");

                        for (Tuple2<Integer, Integer> pair : o._2()){
                                int userj = pair._1();
                                int sim = pair._2();
                                System.out.print("(" +userj+ ","+ sim + ") ");
                        }
                        System.out.println();

                }

		System.out.println("After Finalizing Output:");
		*/

		List<Tuple2<Integer, String>> output3 = resultFinal.collect();
		for(Tuple2<Integer, String> o: output3){

			System.out.println(o._1() + o._2());

		}


		List<String> output4 = lines_to_hold.collect();

		for(String o: output4){

                        System.out.println(o);

                }	

    		spark.stop();
  	}
}
