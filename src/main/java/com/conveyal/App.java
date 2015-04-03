package com.conveyal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.geotools.feature.SchemaException;
import org.geotools.geometry.GeometryBuilder;
import org.hsqldb.persist.Log;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import com.conveyal.osmlib.OSM;
import com.conveyal.trafficengine.Crossing;
import com.conveyal.trafficengine.GPSPoint;
import com.conveyal.trafficengine.OSMUtils;
import com.conveyal.trafficengine.SampleBucket;
import com.conveyal.trafficengine.SampleBucketKey;
import com.conveyal.trafficengine.SpeedSample;
import com.conveyal.trafficengine.SpeedSampleListener;
import com.conveyal.trafficengine.StreetSegment;
import com.conveyal.trafficengine.TrafficEngine;
import com.conveyal.trafficengine.TripLine;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.io.WKTWriter;

public class App 
{
    private static final int MINIMUM_SPEED_SAMPLE_COUNT = 0;
    
	static String PBF_IN = "./data/manila.osm.pbf";//"./data/cebu.osm.pbf";
	static String CSV_IN = "./data/manila-201501-sorted.csv";//"./data/cebu-1m-sorted.csv";
	static String SHAPEFILE_OUT = "./data/manila_streets.shp";
	static String CSV_OUT = "./data/manila_speeds.csv";			
	static String FULL_CSV_OUT = "./data/manila_stats.csv";
	static String DROPOFF_CSV_OUT = "./data/manila_dropoffs.csv";
//	static String PBF_IN = "./data/jakarta.osm.pbf";//"./data/cebu.osm.pbf";
//	static String CSV_IN = "./data/jakarta-2m.csv";//"./data/cebu-1m-sorted.csv";
//	static String SHAPEFILE_OUT = "./data/jakarta_streets.shp";
//	static String CSV_OUT = "./data/jakarta_speeds.csv";			
//	static String FULL_CSV_OUT = "./data/jakarta_stats.csv";
//	static String DROPOFF_CSV_OUT = "./data/jakarta_dropoffs.csv";
	
    public static void main( String[] args ) throws IOException, ParseException, SchemaException
    {

		OSM osm = new OSM(null);
		osm.loadFromPBFFile(PBF_IN);
		
		TrafficEngine te = new TrafficEngine();
		te.setStreets(osm);
		
		outputTriplines( te, "data/manila_triplines.csv");
		//System.exit(0);
		
		List<StreetSegment> ss = te.getStreetSegments( osm );
		
		OSMUtils.toShapefile( ss, SHAPEFILE_OUT );
		
		DB db = DBMaker.newFileDB(new File("samples.db")).make();
		Set<FlatSpeedSample> samples = db.getHashSet("test");
		
		class DbPutter implements SpeedSampleListener{
			
			private Set<FlatSpeedSample> samples;
			int n=0;

			DbPutter(Set<FlatSpeedSample> samples){
				this.samples = samples;
			}

			@Override
			public void onSpeedSample(SpeedSample ss) {
				//samples.add( FlatSpeedSample.fromSpeedSample( ss ) );
				
				n+=1;
				if(n%10000==0){
					//System.out.println( n );
				}
			}
			
		}
		
		te.speedSampleListener = new DbPutter(samples);
		
//		te.speedSampleListener = new SpeedSampleListener(){
//			int n=0;
//			long lastTime = System.currentTimeMillis();
//			@Override
//			public void onSpeedSample(SpeedSample ss) {
//				n += 1;
//				
//				if(n%1000==0){
//					long time = System.currentTimeMillis();
//					double dt = (time-lastTime)/1000.0;
//					double rate = 1000/dt;
//					System.out.println( "rate:"+rate+" records/second");
//					System.out.println( n );
//					//System.out.println( ss );
//					
//					lastTime=time;
//				}
//			}
//			
//		};
		
		ingestCsv(te);
		
		printFullCsv(te);
		
		printSpeedCsv(te);
		
		printDropoffCsv(te);
    }

	private static void printDropoffCsv(TrafficEngine te) throws FileNotFoundException {
		PrintWriter printWriter = new PrintWriter (DROPOFF_CSV_OUT);
		printWriter.println("nd0,nd1,n,frac,geom");
		
		GeometryFactory gf = new GeometryFactory();
		WKTWriter wktw = new WKTWriter();
		
		for(Entry<TripLine, Map<TripLine, Integer>> dropOffEntry : te.getDropOffs().entrySet()){
			TripLine dropOff = dropOffEntry.getKey();
			
			int throughput = te.getNTripEvents( dropOff );
			
			if(throughput==0){
				continue; //TODO if there's a dropoff event we're dealing with here, 
				          //but there are no recorded trip events for this tripline, that's an error.
				          //we should be throwing a runtime error
			}
			
			for(Entry<TripLine,Integer> pickUpEntry : dropOffEntry.getValue().entrySet() ){
				TripLine pickUp = pickUpEntry.getKey();
				Integer n = pickUpEntry.getValue();
				
				Coordinate c1 = dropOff.getGeom().getCentroid().getCoordinate();
				Coordinate c2 = pickUp.getGeom().getCentroid().getCoordinate();
				
				LineString ls = gf.createLineString(new Coordinate[]{c1,c2});
				
				double frac = ((double)n)/throughput;
				
				printWriter.println( String.format("%s,%s,%s,%s,\"%s\"", dropOff.getIdString(), pickUp.getIdString(), n, frac, wktw.write(ls)) );
				
			}
		}
		
		printWriter.close();
	}

	private static void printSpeedCsv(TrafficEngine te) throws FileNotFoundException {
//		// group buckets by wayid:start:end
//		Map<String,List<Entry<SampleBucketKey, SampleBucket>>> bucketGroups = new HashMap<String,List<Entry<SampleBucketKey, SampleBucket>>>();
//		for( Entry<SampleBucketKey, SampleBucket> entry : te.statsSet() ){
//			SampleBucketKey kk = entry.getKey();
//			SampleBucket vv = entry.getValue();
//			
//			String groupKey = kk.wayId+":"+kk.startNodeIndex+":"+kk.endNodeIndex;
//			List<Entry<SampleBucketKey, SampleBucket>> group = bucketGroups.get(groupKey);
//			if(group==null){
//				group = new ArrayList<Entry<SampleBucketKey, SampleBucket>>();
//				bucketGroups.put(groupKey, group);
//			}
//			group.add( entry );
//		}
//		
//		PrintWriter printWriter = new PrintWriter (CSV_OUT);
//		printWriter.println( "waysegid,reverse,count,mean" );
//		for( Entry<String,List<Entry<SampleBucketKey, SampleBucket>>> entry : bucketGroups.entrySet() ){
//			String kk = entry.getKey();
//			List<Entry<SampleBucketKey, SampleBucket>> vv = entry.getValue();
//			
//			String[] parts = kk.split(":");
//			long wayId = Long.parseLong(parts[0]);
//			int startNodeIndex = Integer.parseInt(parts[1]);
//			int endNodeIndex = Integer.parseInt(parts[2]);
//			
//			int nd0 = startNodeIndex;
//			int nd1 = endNodeIndex;
//			boolean reverse = endNodeIndex<startNodeIndex;
//			if(reverse){
//				int tmp=nd1;
//				nd1=nd0;
//				nd0=tmp;
//			}
//			
//			// sum every bucket
//			int count=0;
//			int meansum=0;
//			for(Entry<SampleBucketKey, SampleBucket> bucketEntry : vv ){
//				SampleBucketKey bk = bucketEntry.getKey();
//				SampleBucket bucket = bucketEntry.getValue();
//				
////				if(bk.hourOfWeek%24 >= 6 && bk.hourOfWeek%24 <= 9) {
//				if(!reverse){
//					count += bucket.count;
//					meansum += bucket.count*bucket.mean;
////				}
//				}
//			}
//			
//			if(count>MINIMUM_SPEED_SAMPLE_COUNT){
//				printWriter.println( String.format("%d.%d.%d,%s,%d,%f", wayId, nd0, nd1, reverse, count, ((float)meansum)/count) );
//			}
//		}
//		printWriter.close();
	}

	private static void printFullCsv(TrafficEngine te) throws FileNotFoundException {
//		PrintWriter fullWriter = new PrintWriter (FULL_CSV_OUT);
//		for( Entry<SampleBucketKey, SampleBucket> entry : te.statsSet() ){
//			SampleBucketKey key = entry.getKey();
//			SampleBucket val = entry.getValue();
//			
//			fullWriter.print(String.format("%s:%s:%s,%s,",key.wayId,key.startNodeIndex,key.endNodeIndex,key.hourOfWeek));
//			
//			fullWriter.print(String.format("%s,",val.mean));
//			
////			for(int count : val.buckets){
////				fullWriter.print(","+count);
////			}
//			fullWriter.print("\n");
//		}
//		fullWriter.close();
	}

	private static void ingestCsv(TrafficEngine te) throws IOException, ParseException {
		File csvData = new File(CSV_IN);
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

			long time = parseTaxiTimeStrToMicros( timeStr );

			GPSPoint pt = new GPSPoint(time, vehicleId, Double.parseDouble(lonStr), Double.parseDouble(latStr));
			te.update( pt );

			i++;
		}
		
		System.out.println( "DONE" );
	}
    
	private static void outputTriplines(TrafficEngine te, String fnout) throws FileNotFoundException {
		WKTWriter writer = new WKTWriter();
		PrintWriter printWriter = new PrintWriter (fnout);
		printWriter.println( "wayid,clusterid,geom" );
		
		List<TripLine> triplines = te.getTripLines();
		for( TripLine tl : triplines ){
			LineString ls = tl.getGeom();
			String wkt = writer.write(ls);
			
			printWriter.println( String.format("%s,%s,\"%s\"", tl.getWayId(), tl.getClusterIndex(), wkt) );
		}
		
		printWriter.close();
	}

	private static List<GPSPoint> loadGPSPointsFromCSV(String string) throws IOException, ParseException {
		List<GPSPoint> ret = new ArrayList<GPSPoint>();
		
		File csvData = new File(string);
		CSVParser parser = CSVParser.parse(csvData, Charset.forName("UTF-8"), CSVFormat.RFC4180);

		int i = 0;
		for (CSVRecord csvRecord : parser) {
			if (i % 10000 == 0) {
				System.out.println(i);
			}

			String timeStr = csvRecord.get(0);
			String vehicleId = csvRecord.get(1);
			String lonStr = csvRecord.get(2);
			String latStr = csvRecord.get(3);

			long time = parseTaxiTimeStrToMicros( timeStr );

			GPSPoint pt = new GPSPoint(time, vehicleId, Double.parseDouble(lonStr), Double.parseDouble(latStr));
			ret.add(pt);

			i++;
		}

		return ret;
	}

	private static long parseTaxiTimeStrToMicros(String timeStr) throws ParseException {
		StringBuilder sb = new StringBuilder(timeStr);
		int snipStart = sb.indexOf(".");
		int snipEnd = sb.indexOf("+");
		String microsString="0.0";
		if (snipStart != -1) {
			microsString = "0"+sb.substring(snipStart,snipEnd);
			sb.delete(snipStart,snipEnd);
			timeStr = sb.toString();
		}
			
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssX");
		
		Date dt = formatter.parse(timeStr);
		long timeMillis = dt.getTime();
		long micros = (long) (Double.parseDouble(microsString)*1000000);
		
		long time = timeMillis*1000 + micros;
		return time;
	}
}
