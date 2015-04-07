package com.conveyal;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Map.Entry;

import org.mapdb.Fun.Tuple3;

public class InspectHistogram {

	public static void main(String[] args) throws FileNotFoundException {
		String HISTOGRAM_FILENAME = "./data/manila_histogram.db";
		String CSV_OUT_FILENAME = "./data/manila_counts.csv";
		
		Histogram hist = new Histogram(HISTOGRAM_FILENAME);
						
		PrintWriter printWriter = new PrintWriter(CSV_OUT_FILENAME);
		printWriter.println("seg_id,count");
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
