package com.oq_io.cs_challenge;

import java.io.File;
import java.util.List;
import com.oq_io.cs_challenge.dao.Event;
import com.oq_io.cs_challenge.utils.DbConnect;
import com.oq_io.cs_challenge.utils.LogParser;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * json
     */
    public void testJsonParse()
    {
    	File testJson = new GenerateRandomEventLogs(100).getTempFile();
    	DbConnect memDb = new DbConnect();
    	LogParser parseTest = new LogParser(testJson, memDb);
    	int rowsRead = parseTest.getStatsProcRows();
    	List<Event> rows = memDb.SelectDataObj(Event.class);
    	
    	assertEquals( rowsRead, rows.size());
    }
    
    
}
