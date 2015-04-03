package com.conveyal;

import java.io.Serializable;

import com.conveyal.trafficengine.SpeedSample;
import com.conveyal.trafficengine.TripLine;

public class FlatSpeedSample implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2449504828222910336L;
	public long wayid;
	public int fromnd;
	public int tond;
	public double speed;
	public long time;

	public FlatSpeedSample(long wayid, int fromnd, int tond, double speed, long time) {
		this.wayid=wayid;
		this.fromnd=fromnd;
		this.tond=tond;
		this.speed=speed;
		this.time=time;
	}

	public static FlatSpeedSample fromSpeedSample(SpeedSample ss) {
		TripLine tl = ss.getFirstCrossing().getTripline();
		
		long wayid = tl.getWayId();
		int fromnd = tl.getNdIndex();
		int tond = ss.getLastCrossing().getTripline().getNdIndex();
		double speed = ss.getSpeed();
		long time = ss.getLastCrossing().getTimeMicros();
		
		return new FlatSpeedSample(wayid,fromnd,tond,speed,time);
	}

}
