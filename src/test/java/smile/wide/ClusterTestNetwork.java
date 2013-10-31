package smile.wide;

import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;

import smile.wide.BNQuery;
import smile.wide.Network;
import smile.wide.facebook.FacebookCSVReader;
import smile.wide.facebook.FacebookDataset;


/** Test for the main Network class: performs inference on the Facebook dataset
 * 
 * @author tomas.singliar@boeing.com
 *
 */
public class ClusterTestNetwork {

	// in the HDFS, this is big data 
	private static String DATASET_LOCATION = "hdfs:///DataSets/Facebook/users/FB_Users_34579801_to_50000000_pulled_2012-10-03.zip";
	
	// in the local filesystem, the idea is for the programmer to pretend
	// this all happens like Hadoop didn't exist
	private static String NETWORK_LOCATION = "input/Facebook.34579801_to_50000000.xdsl";
	
	
	// =====================================================================================

	@Test
	public void testNetworkInstantiation() {
		System.out.println("in TestNetwork.testNetworkInstantiation");
		Network net = new Network();
		System.out.println("number of nodes in empty network:" + net.getNodeCount());
	}
	
	
	@Test
	public void test() {
		
		// set up the Facebook dataset
		FacebookDataset ds = null;
		try {
			ds = new FacebookDataset(new URI(DATASET_LOCATION));
		} catch (URISyntaxException e) {
			fail("Facebook dataset failed.");
		}		

		// set up the dataset reader
		FacebookCSVReader reader = new FacebookCSVReader();
				
		// set up the network
		Network net = new Network(NETWORK_LOCATION);
		
		System.out.println("Setting up query.");
		// set up the query
		BNQuery query = new BNQuery();
		query.setQueryVar("Age");
		query.addEvidenceVariable("IsAppUser");
		query.addEvidenceVariable("LikesCount");
		query.addEvidenceVariable("FriendsCount");
		query.addEvidenceVariable("Sex");
				
		System.out.println("Running inference.");
		net.infer(ds, reader, query);
		
		System.out.println("Retrieving result.");
		net.retrieveResult();
		float[][] result = net.inferenceResult(); 
		
		// assert things about the result, like
		// - there are as many columns in each row as the outcomes of Age
		// - that the rows sum to one
		
		assertTrue(result.length > 0);
		assertTrue(result[0].length == 
						net.getNetwork().getOutcomeCount("Age"));		
	}
	
	// ==========================================
	
	/** run the test from the command line
	 *  with all the paths set up
	 */
	public static void main(String args[])
	{
		(new ClusterTestNetwork()).test();
	}

}
