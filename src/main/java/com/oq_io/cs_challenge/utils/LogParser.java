package com.oq_io.cs_challenge.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.oq_io.cs_challenge.dao.Event;

public class LogParser {
	private File logFile;
	private DbConnect dbConnect;
	private long statsProcTime;
	private int statsProcRows = 0;
	private static Logger logger = LogManager.getLogger(LogParser.class);
	
	/**
	   * Constructor takes json log file and configured database pool, streams text to parsed json objects
	   * Then inserts into Event table configured with database pool
	   * Then runs EventProcessor to execute db level business rules to calculate duration and alert of paired evetns
	   * @param logPath input json file of events
	   * @param dbConnect created database pool
	   */
	
	public LogParser(File logPath, DbConnect dbConnect) {
		this.logFile = logPath;
		this.dbConnect = dbConnect;
		parseLog();
	}
	

	private void parseLog() {
		long start = System.currentTimeMillis();
		try {            
			InputStream inputStream = new FileInputStream(logFile);
            JsonReader reader = new JsonReader(new InputStreamReader(inputStream));
	        reader.beginArray();
	        while (reader.hasNext()) {
	            Event event = new Gson().fromJson(reader, Event.class);
	            logger.debug("event obj: "+event.getLog());
	            dbConnect.Insert(event, reader.hasNext());
	            statsProcRows++;
	        }
	        reader.endArray();
	        this.statsProcTime = System.currentTimeMillis() - start;
	    }catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}

	
	/**
	   * This is the main method which makes use of addNum method.
	   * @return time of json parsing for information.
	   */
	public long getStatsProcTime() {
		return statsProcTime;
	}
	
	/**
	   * This is the main method which makes use of addNum method.
	   * @return no. of json objects constructed for information.
	   */

	public int getStatsProcRows() {
		return statsProcRows;
	}

	
}
