package com.conveyal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
import com.conveyal.trafficengine.SpeedSample;
import com.conveyal.trafficengine.StreetSegment;
import com.conveyal.trafficengine.TrafficEngine;
import com.conveyal.trafficengine.TripLine;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.io.WKTWriter;

public class App 
{    
	static String PBF_IN = "./data/manila.osm.pbf";//"./data/cebu.osm.pbf";
	static String CSV_IN = "./data/manila-201501-sorted.csv";//"./data/cebu-1m-sorted.csv";
	static String SHAPEFILE_OUT = "./data/manila_streets.shp";
	static String CSV_OUT = "./data/manila_speeds.csv";			
	static String FULL_CSV_OUT = "./data/manila_stats.csv";
	static String DROPOFF_CSV_OUT = "./data/manila_dropoffs.csv";
	static String TRIPLINE_CSV_OUT = "./data/manila_triplines.csv";
	static String HISTOGRAM_DB = "./data/manila_histogram.db";
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
		
		outputTriplines( te, TRIPLINE_CSV_OUT);
		
		List<StreetSegment> ss = te.getStreetSegments( osm );
		OSMUtils.toShapefile( ss, SHAPEFILE_OUT );
		
		Histogram hist = new Histogram(HISTOGRAM_DB);
		
		ingestCsv(hist, te);
		
		//printDropoffCsv(te);
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

	private static void ingestCsv(Histogram hist, TrafficEngine te) throws IOException, ParseException {
		File csvData = new File(CSV_IN);
		CSVParser parser = CSVParser.parse(csvData, Charset.forName("UTF-8"), CSVFormat.RFC4180);
		
		long deadline = 0;
		long WINDOW_SIZE = 5*60*1000000; // five minutes, in microseconds
		List<SpeedSample> staging = new ArrayList<SpeedSample>();
		for (CSVRecord csvRecord : parser) {

			String timeStr = csvRecord.get(0);
			String vehicleId = csvRecord.get(1);
			String lonStr = csvRecord.get(2);
			String latStr = csvRecord.get(3);

			long time = parseTaxiTimeStrToMicros( timeStr );
			
			if( time > deadline ){
				SimpleDateFormat sdf = new SimpleDateFormat();
				System.out.println( staging.size()+" records rolled at "+sdf.format(new Date(deadline/1000)) );
				
				// roll logs
				hist.ingest( staging );
				staging.clear();
				
				// set new deadline
				deadline = ((time/WINDOW_SIZE)+1)*WINDOW_SIZE;
			}

			GPSPoint pt = new GPSPoint(time, vehicleId, Double.parseDouble(lonStr), Double.parseDouble(latStr));
			List<SpeedSample> speeds = te.update( pt );
			
			if(speeds != null ){
				staging.addAll( speeds );
			}

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
