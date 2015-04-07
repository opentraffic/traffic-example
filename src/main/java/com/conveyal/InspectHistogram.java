package com.conveyal;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;

import org.mapdb.Fun.Tuple3;

public class InspectHistogram {

	public static void main(String[] args) throws FileNotFoundException {
		Histogram hist = new Histogram();
						
		PrintWriter printWriter = new PrintWriter ("manila_histspeeds.csv");
		String curWayId = null;
		for(Entry<Tuple3<String, Integer, Integer>, Integer> entry : hist.map.entrySet()){
			Tuple3<String, Integer, Integer> key = entry.getKey();
			String wayId = key.a;
			
			if( !wayId.equals( curWayId ) ){
				double meanSpeed = hist.meanSpeed( wayId );
				
				printWriter.println( wayId+","+meanSpeed );
				printWriter.flush();
				
				curWayId = wayId;
			}
		}
		
		printWriter.close();
	}

}
