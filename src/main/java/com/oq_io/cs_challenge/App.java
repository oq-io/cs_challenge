package com.oq_io.cs_challenge;

import java.io.File;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.apache.logging.log4j.LogManager;
import com.oq_io.cs_challenge.dao.Event;
import com.oq_io.cs_challenge.utils.DbConnect;
import com.oq_io.cs_challenge.utils.EventProcessor;
import com.oq_io.cs_challenge.utils.LogParser;

/**
* Simple event log parse logic focusing on limited memory usage and transactional database calls
* Uses Hibernate entity creation currently configured for jdbc:hsqldb:mem
* Log file streaming and Database batch transactions for resource comptability
* @author  oq_io
* @version 0.1
* @since   2022-03-13
*/


public class App {
	private static Logger logger = LogManager.getLogger(App.class);
	
	 /**
	   * This is the main method calling App.class
	   * @param args [0] file input json file of events.
	   * @return Nothing.
	   * @exception catch if args file does not exist
	   */
	public static void main(String[] args) {
		
		if (args == null || args.length != 1) {
            logger.error("Arguments should be a FilePath.");
            throw new IllegalArgumentException("Please check the arguments and run again.");
        }
		
		File temp = new File(args[0]);
		if(temp.exists()) {
			logger.info("Working on: "+temp.getAbsolutePath());
			new App(temp);
		}else {
			logger.error(args[0]+" is not a file");
		 }
		
	}
	
	
	 /**
	   * Class take File input calls LogParser to read json and insert to db configured
	   * Then runs EventProcessor to execute db level business rules to calculate duration and alert of paired evetns
	   * @param file input json file of events
	   */
	
	public App(File file) {
		DbConnect dbConnect = new DbConnect();
		LogParser parser = new LogParser(file,dbConnect);
		logger.info(parser.getStatsProcRows()+" rows, processed in "+parser.getStatsProcTime()+"ms");
		
		EventProcessor eventProcess = new EventProcessor(dbConnect);
		logger.info(eventProcess.getStatsProcRows()+" events' duration calculated, processed in "+eventProcess.getStatsProcTime()+"ms");
		
				
		Session session = dbConnect.getSession();
		
		Long totalEvents = (Long) session.createQuery("SELECT count(*) from Event").getSingleResult();
		logger.info("Totals events: "+totalEvents);
		
		String completeEventsQuery = "select count(*) "
		        + " from Event eS "
		        + " join Event eF ON eF.id = eS.id AND eF.state = 'FINISHED' AND eS.state = 'STARTED'";
		Long completeEvents = (Long) session.createQuery(completeEventsQuery).getSingleResult();
		logger.info("Totals complete events: "+completeEvents);
		
		
		
		String orphanEventsQuery = "select count(*) from Event where id not in ("
		        + " select eS.id from Event eS join Event eF ON eF.id = eS.id AND eF.state = 'FINISHED' AND eS.state = 'STARTED')";
		Long orphanEvents = (Long) session.createQuery(orphanEventsQuery).getSingleResult();
		logger.info("Totals orphan events: "+orphanEvents);
		
		
		
		Double avgDuration = (Double) session.createQuery("SELECT avg(duration) from Event where state = :state")
				.setParameter("state", Event.State.FINISHED)
				.getSingleResult();
		
		logger.info("Average durarion of events: "+String.format("%.2f",avgDuration)+"ms");


		session.close();
	

    }
}
