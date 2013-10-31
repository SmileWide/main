/**
 * Copyright © 2013 Boeing. Unpublished Work. All Rights Reserved.
 */
package smile.wide;

import static org.junit.Assert.*;

import java.util.Map;
import org.apache.log4j.Level;
import org.junit.Test;	// JUnit4, that is

import smile.Network;
import smile.wide.NaiveBayes;


/**
 * JUnit4 test case for NaiveBayes2 class.
 * 
 * @author robert.e.cranfill@boeing.com
 */
public class TestNaiveBayes
{

/**
 * Test the structure of the test dataset. (Useful?)
 */
@Test
public void testSmallNetStructure()
	{
	NaiveBayes.setLoggerLevel(Level.OFF);
	Network testnet = NaiveBayes.smallNet();
	
	assertTrue(testnet.getNodeCount() == 3);
	
//	int nodeid = testnet.getNode("Class");
//	System.out.printf("node id is %d \n", nodeid);
//	assertTrue(nodeid == 0);	// not sure if we can assume this
	
	assertTrue(testnet.getOutcomeCount("Class") == 2);

	// TODO: more
	
	}


/**
 * Test a simple inference.
 * If I am fat and drink a lot, how likely am I to be happy?
 */
@Test
public void testOneInstance()
	{
	NaiveBayes.setLoggerLevel(Level.OFF);
	Network testnet = NaiveBayes.smallNet();
	
	double[] post = NaiveBayes.infer(testnet, new Integer(0), new Integer(0));
	assertTrue(post.length == 2);
	
//	System.out.printf("post[0] = %f \n", post[0]);
//	System.out.printf("post[1] = %f \n", post[1]);
	
	assertEquals(post[0], 2.0/3, 0.00001);
	assertEquals(post[1], 1.0/3, 0.00001);
	}


/**
 * Using the dataset 'happiness.txt', do some inference.
 * 	Expected count of Happy: 36424.304
 * 	Expected count of   Sad: 63575.696
 */
@Test
public void testHappinessFile()
	{
	NaiveBayes.setLoggerLevel(Level.WARN);
	Network theNet = NaiveBayes.smallNet();

	String filename = "input/Happiness.txt";
	Map<Integer, double[]> result = NaiveBayes.allPosteriorsFromFile(theNet, filename);
//	System.out.printf("Read %d items from '%s'.\n", result.size(), filename);
	
	double happyCount = 0.0;
	double sadCount = 0.0;
	for (Integer key : result.keySet())
		{
		// System.out.printf("%d : (%4.3f, %4.3f)\n", key, result.get(key)[0], result.get(key)[1]);
		happyCount += result.get(key)[0];
		sadCount += result.get(key)[1];
		}
	
//	System.out.printf("Expected count of Happy: %4.4f\n", happyCount);
//	System.out.printf("Expected count of   Sad: %4.4f\n", sadCount);

	assertEquals(happyCount, 36424.304, 0.0005);
	assertEquals(sadCount, 63575.696, 0.0005);
	
	}
	

}
