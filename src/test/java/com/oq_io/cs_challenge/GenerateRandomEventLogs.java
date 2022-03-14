package com.oq_io.cs_challenge;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.oq_io.cs_challenge.dao.Event;

public class GenerateRandomEventLogs {
	private File tempFile;
	
	private static Logger logger = LogManager.getLogger(GenerateRandomEventLogs.class);
	
	public static void main(String[] args) {
		new GenerateRandomEventLogs(100);
	}
	
	public GenerateRandomEventLogs() {
		writeFileToTestParse(1000);
	}
	
	public GenerateRandomEventLogs(int numLogs) {
		writeFileToTestParse(numLogs);
	}
	
	public File getTempFile() {
		return tempFile;
	}
	
	public void writeFileToTestParse(int numLogs ) {
    	List<Event> eventList = new ArrayList<Event>();
    	List<Event> startAndFinishPair;
    	for (int i = 0; i < numLogs; i++) {
    		startAndFinishPair = createRandomEventPair();
    		eventList.add(startAndFinishPair.get(0));
    		eventList.add(startAndFinishPair.get(1));
    	}
    	
    	logger.info(eventList.size());
    	logger.info(eventList);
    			
		try {
			this.tempFile = File.createTempFile("logevent-test", ".json");
			Writer writer = new FileWriter(tempFile);
			Gson gson = new Gson();
			logger.info(gson.toJson(eventList));
	    	gson.toJson(eventList,writer);
	    	writer.flush();
	    	logger.info(tempFile.getAbsoluteFile());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
    	
    }
    
    private List<Event> createRandomEventPair() {
  
        String id = RandomStringUtils.random(10, true, true);
    	//String state = Event.State.values()[new Random().nextInt(Event.State.values().length)].toString();
        String startState = Event.State.values()[0].toString();
        String finState = Event.State.values()[1].toString();
    	String type = RandomStringUtils.random(10, true, false)+"_LOG";
    	String host = RandomStringUtils.random(10, true, true);
    	
    	Instant tsNow = Instant.now();
    	long startTs = tsNow.minus(2, ChronoUnit.HOURS).toEpochMilli();
    	long nowEndThreshold = tsNow.toEpochMilli();
    	//get random start from last 2 hours
    	long randomStartTs = ThreadLocalRandom.current().nextLong(startTs, nowEndThreshold);
  	
    	//get threshold for end from random start +8 ms - 50/50 change > 4 ms
    	long randomEndThreshold = Instant.ofEpochMilli(randomStartTs).plus(8,ChronoUnit.MILLIS).toEpochMilli();
    	long randomEndTs = ThreadLocalRandom.current().nextLong(randomStartTs, randomEndThreshold);
    	    	
    	List<Event> returnPair = new ArrayList<Event>();
    	returnPair.add(new Event(id, startState, type, host, randomStartTs));
    	returnPair.add(new Event(id, finState, type, host, randomEndTs));
    	return returnPair;
    }
	
	
	
}
