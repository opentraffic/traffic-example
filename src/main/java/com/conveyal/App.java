package com.conveyal;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.geotools.feature.SchemaException;

import com.conveyal.osmlib.OSM;
import com.conveyal.trafficengine.GPSPoint;
import com.conveyal.trafficengine.OSMUtils;
import com.conveyal.trafficengine.SampleBucket;
import com.conveyal.trafficengine.SampleBucketKey;
import com.conveyal.trafficengine.SpeedSample;
import com.conveyal.trafficengine.SpeedSampleListener;
import com.conveyal.trafficengine.StreetSegment;
import com.conveyal.trafficengine.TrafficEngine;

public class App 
{
    public static void main( String[] args ) throws IOException, ParseException, SchemaException
    {
		OSM osm = new OSM(null);
		osm.loadFromPBFFile("./data/cebu.osm.pbf");
		
		TrafficEngine te = new TrafficEngine();
		te.setStreets(osm);
		
		List<StreetSegment> ss = te.getStreetSegments( osm );
		
		OSMUtils.toShapefile( ss, "segs.shp" );
		
		List<GPSPoint> gpsPoints = loadGPSPointsFromCSV("./data/cebu-1m-sorted.csv");

		
		te.speedSampleListener = new SpeedSampleListener(){
			int n=0;
			long lastTime = System.currentTimeMillis();
			@Override
			public void onSpeedSample(SpeedSample ss) {
				n += 1;
				
				if(n%1000==0){
					long time = System.currentTimeMillis();
					double dt = (time-lastTime)/1000.0;
					double rate = 1000/dt;
					System.out.println( "rate:"+rate+" records/second");
					System.out.println( n );
					//System.out.println( ss );
					
					lastTime=time;
				}
			}
			
		};
		
		int j=0;
		for (GPSPoint gpsPoint : gpsPoints) {
			j++;
			if(j%1000==0){
				System.out.println(String.format("%d/%d gps point read", j, gpsPoints.size()));
			}
			te.update(gpsPoint);
		}
		
		// group buckets by wayid:start:end
		Map<String,List<Entry<SampleBucketKey, SampleBucket>>> bucketGroups = new HashMap<String,List<Entry<SampleBucketKey, SampleBucket>>>();
		for( Entry<SampleBucketKey, SampleBucket> entry : te.statsSet() ){
			SampleBucketKey kk = entry.getKey();
			SampleBucket vv = entry.getValue();
			
			String groupKey = kk.wayId+":"+kk.startNodeIndex+":"+kk.endNodeIndex;
			List<Entry<SampleBucketKey, SampleBucket>> group = bucketGroups.get(groupKey);
			if(group==null){
				group = new ArrayList<Entry<SampleBucketKey, SampleBucket>>();
				bucketGroups.put(groupKey, group);
			}
			group.add( entry );
		}
		
		PrintWriter printWriter = new PrintWriter ("out.csv");
		for( Entry<String,List<Entry<SampleBucketKey, SampleBucket>>> entry : bucketGroups.entrySet() ){
			String kk = entry.getKey();
			List<Entry<SampleBucketKey, SampleBucket>> vv = entry.getValue();
			
			String[] parts = kk.split(":");
			long wayId = Long.parseLong(parts[0]);
			int startNodeIndex = Integer.parseInt(parts[1]);
			int endNodeIndex = Integer.parseInt(parts[2]);
			
			int nd0 = startNodeIndex;
			int nd1 = endNodeIndex;
			boolean reverse = endNodeIndex<startNodeIndex;
			if(reverse){
				int tmp=nd1;
				nd1=nd0;
				nd0=tmp;
			}
			
			// sum every bucket
			int count=0;
			int meansum=0;
			for(Entry<SampleBucketKey, SampleBucket> bucketEntry : vv ){
				SampleBucket bucket = bucketEntry.getValue();
				
				count += bucket.count;
				meansum += count*bucket.mean;
			}
			
			printWriter.println( String.format("%d.%d.%d,%s,%d,%f", wayId, nd0, nd1, reverse, count, ((float)meansum)/count) );
		}
		printWriter.close();
    }
    
	private static List<GPSPoint> loadGPSPointsFromCSV(String string) throws IOException, ParseException {
		List<GPSPoint> ret = new ArrayList<GPSPoint>();
		
		File csvData = new File(string);
		CSVParser parser = CSVParser.parse(csvData, Charset.forName("UTF-8"), CSVFormat.RFC4180);

		DateFormat formatter = new TaxiCsvDateFormatter();

		int i = 0;
		for (CSVRecord csvRecord : parser) {
			if (i % 10000 == 0) {
				System.out.println(i);
			}

			String timeStr = csvRecord.get(0);
			String vehicleId = csvRecord.get(1);
			String lonStr = csvRecord.get(2);
			String latStr = csvRecord.get(3);

			Date dt = formatter.parse(timeStr);
			long time = dt.getTime();

			GPSPoint pt = new GPSPoint(time, vehicleId, Double.parseDouble(lonStr), Double.parseDouble(latStr));
			ret.add(pt);

			i++;
		}

		return ret;
	}
}
