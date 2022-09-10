package com.RUSpark;

public class Point implements Comparable<Point>{

	public int x, y;
	public Point(int x, int y){

		this.x = x;
		this.y = y;

	}

	//return 1 of a > b
	//else return 0
	@Override
	public int compareTo (Point a){

		//System.out.println( "Compare To: ("+x+","+y+")"+a);
		if (a.y > y){
			//System.out.println("1. returning 1");
			return 1;
		}else if(a.y == y){
			if(a.x < x){
				//System.out.println("2. returning 1");
				return 1;
			}else if (a.x > x){
				//System.out.println("1. returning 0");
				return -1;
			}else{
				return 0;
			}
		}	
		//System.out.println("2. returning 0");
		return -1;
	}

	@Override
	public String toString(){
		//String result = 
		return "(" + x+","+ y + ")";

	}

	//public static void main(String[] args){

		


	//}



}
