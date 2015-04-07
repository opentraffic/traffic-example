package com.conveyal;

import java.io.File;
import java.util.Calendar;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Fun.Tuple3;

import com.conveyal.trafficengine.SpeedSample;
import com.conveyal.trafficengine.TripLine;

public class Histogram {
	private static final double BUCKET_SIZE = 0.5; // in m/s
	
	
	DB db;
	//(way_semgent_id,hour_of_week,speed_histogram_bucket)
	ConcurrentNavigableMap<Tuple3<String,Integer,Integer>,Integer> map;
	
	Histogram(String filename){
		db = DBMaker.newFileDB(new File(filename))
		           .closeOnJvmShutdown()
		           .make();
		
		 map = db.getTreeMap("collectionName");
	}
	
	public ConcurrentNavigableMap<Tuple3<String, Integer, Integer>, Integer> getHistogramForWay(String waySegId){
		return map.subMap(new Tuple3<String,Integer,Integer>(waySegId, null, null), new Tuple3<String,Integer,Integer>(waySegId, Integer.MAX_VALUE, Integer.MAX_VALUE));
	}
	
	public double meanSpeed(String waySegId){
		int count=0;
		double sumSpeed=0;
		
		for(Entry<Tuple3<String, Integer, Integer>, Integer> entry : this.getHistogramForWay(waySegId).entrySet() ){
			int speedBucket = entry.getKey().c;
			double speed = (speedBucket+0.5)*BUCKET_SIZE;
			count += entry.getValue();
			sumSpeed += speed;
		}
		
		return sumSpeed/count;
	}
	
	public double count(String waySegId){
		int count=0;
		
		for(Entry<Tuple3<String, Integer, Integer>, Integer> entry : this.getHistogramForWay(waySegId).entrySet() ){
			count += entry.getValue();
		}
		
		return count;
	}

	public void ingest(List<SpeedSample> staging) {
		for(SpeedSample ss : staging){
			Tuple3<String,Integer,Integer> key = getKey( ss );
			Integer count = map.get(key);
			if(count==null){
				count=0;
			}
			map.put(key, count+1);
		}
		db.commit();
	}

	private Tuple3<String, Integer, Integer> getKey(SpeedSample ss) {
		String waySegmentId = getWaySegmentId( ss );
		int hourOfWeek = getHourOfWeek(ss.getLastCrossing().getTimeMicros());
		int histogramBucket = (int) (ss.getSpeed()/BUCKET_SIZE);
		
		return new Tuple3<String, Integer, Integer>(waySegmentId,hourOfWeek,histogramBucket);
	}
	
	private String getWaySegmentId(SpeedSample ss) {
		TripLine tl0 = ss.getFirstCrossing().getTripline();
		TripLine tl1 = ss.getLastCrossing().getTripline();
		
		return tl0.getWayId()+"."+tl0.getNdIndex()+"."+tl1.getNdIndex();
	}

	private int getHourOfWeek(long timeMicros) {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis( timeMicros/1000 );
		
		int dowVal = c.get(Calendar.DAY_OF_WEEK);
		int dow=0;
		switch(dowVal){
		case Calendar.SUNDAY: dow=0; break;
		case Calendar.MONDAY: dow=1; break;
		case Calendar.TUESDAY: dow=2; break;
		case Calendar.WEDNESDAY: dow=3; break;
		case Calendar.THURSDAY: dow=4; break;
		case Calendar.FRIDAY: dow=5; break;
		case Calendar.SATURDAY: dow=6; break;
		}
		
		int hourOfDay = c.get(Calendar.HOUR_OF_DAY);
		
		return dow*24+hourOfDay;
	}

}
