package com.conveyal;

import java.util.Map.Entry;

import org.mapdb.Fun.Tuple3;

public class InspectHistogram {

	public static void main(String[] args) {
		Histogram hist = new Histogram();
		
		for(Entry<Tuple3<String, Integer, Integer>, Integer> blah : hist.map.entrySet()){
			System.out.println( blah.getKey()+","+blah.getValue() );
		}
	}

}
