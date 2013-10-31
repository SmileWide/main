/*
             Licensed to the DARPA XDATA project.
       DARPA XDATA licenses this file to you under the 
         Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
           You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
                 either express or implied.                    
   See the License for the specific language governing
     permissions and limitations under the License.
 */
package smile.wide;

import smile.Network;
import smile.wide.data.Instance;
import smile.wide.obsolete.GenieFileReader;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * A simple test/example of using the Network class.
 * 
 * @author tomas.singliar@boeing.com
 * @author robert.e.cranfill@boeing.com
 */
public class NaiveBayes
{

private static final Logger s_logger;

static
	{
	s_logger = Logger.getLogger(NaiveBayes.class);
	s_logger.setLevel(Level.DEBUG);
	}


public static void setLoggerLevel(Level l)
	{
	s_logger.setLevel(l);
	}


/**
 * Build a little naive net about thin happy and sad fat people who drink
 * 
 * @return The Network.
 */
public static smile.Network smallNet()
	{
	s_logger.debug("Creating the Network");

	Network theNet = new Network();

	s_logger.debug("Adding the first node");

	int clsNodeID = theNet.addNode(Network.NodeType.Cpt, "Class");
	s_logger.debug("Adding outcomes");
	theNet.addOutcome(clsNodeID, "Happy"); // there are 2 outcomes there
	theNet.addOutcome(clsNodeID, "Sad"); // already by default
	s_logger.debug("Deleting original outcomes");
	theNet.deleteOutcome(clsNodeID, 0); // we have created new ones, so
	theNet.deleteOutcome(clsNodeID, 0); // delete the two original

	s_logger.debug("Setting first CPT");
	double[] prior = new double[2];
	prior[0] = 0.4;
	prior[1] = 0.6;
	theNet.setNodeDefinition(clsNodeID, prior);

	s_logger.debug("Adding the next node");

	// double[] def = theNet.getNodeDefinition(clsNodeID);
	// int sz = def.length;
	// System.out.printf("Class node has %d outcomes.\n", sz);

	int drinkNodeID = theNet.addNode(Network.NodeType.Cpt, "Drinks");
	theNet.setOutcomeId(drinkNodeID, 0, "Lots");
	theNet.setOutcomeId(drinkNodeID, 1, "Little");
	theNet.addOutcome(drinkNodeID, "None");

	// System.out.printf("Drink node has %d outcomes.\n",
	// theNet.getNodeDefinition(drinkNodeID).length);

	s_logger.debug("Adding the third node");

	int sizeNodeID = theNet.addNode(Network.NodeType.Cpt, "Size");
	theNet.addOutcome(sizeNodeID, "Fat");
	theNet.addOutcome(sizeNodeID, "Thin");
	theNet.deleteOutcome(sizeNodeID, 0);
	theNet.deleteOutcome(sizeNodeID, 0);
	// System.out.printf("Size node has %d outcomes.\n",
	// theNet.getNodeDefinition(sizeNodeID).length);

	s_logger.debug("Adding the arcs");

	theNet.addArc(clsNodeID, drinkNodeID);
	theNet.addArc(clsNodeID, sizeNodeID);

	s_logger.debug("Setting the second CPT");

	double[] drinkCPT = new double[2 * 3];
	drinkCPT[0 * 3 + 0] = 0.3;
	drinkCPT[0 * 3 + 1] = 0.6;
	drinkCPT[0 * 3 + 2] = 0.1;

	drinkCPT[1 * 3 + 0] = 0.4;
	drinkCPT[1 * 3 + 1] = 0.2;
	drinkCPT[1 * 3 + 2] = 0.4;
	theNet.setNodeDefinition(drinkNodeID, drinkCPT);

	s_logger.debug("Setting the third CPT");

	double[] sizeCPT = new double[2 * 2];
	sizeCPT[0 * 2 + 0] = 0.8;
	sizeCPT[0 * 2 + 1] = 0.2;
	sizeCPT[1 * 2 + 0] = 0.2;
	sizeCPT[1 * 2 + 1] = 0.8;
	theNet.setNodeDefinition(sizeNodeID, sizeCPT);

	s_logger.debug("Almost done");

	return theNet;
	}


/**
 * Do the inference.
 * @param theNet
 * @param size
 * @param drink
 * @return	An array of values.
 */
public static double[] infer(Network theNet, Integer size, Integer drink)
	{
	theNet.clearAllEvidence();
	if (size != null)
		{
		theNet.setEvidence(theNet.getNode("Drinks"), size);
		}
	if (drink != null)
		{
		theNet.setEvidence(theNet.getNode("Size"), drink);
		}

	theNet.updateBeliefs();

	double[] posterior = theNet.getNodeValue(theNet.getNode("Class"));
	return posterior;
	}


/**
 * Read a file and run an inference on each instance found within.
 * <ol>
 * <li>Reads a file. Instances should have unique identifiers, which are ints;
 * <li>Runs inference on the instances, sequentially ;
 * <li>Produces the map.
 * </ol>
 * 
 * @param theNet	The BN with which to infer.
 * @param fileName	The file containing instances.
 * 
 * @return A map of (instance IDs) -> (the inferences for that ID)
 **/
public static Map<Integer, double[]> allPosteriorsFromFile(Network theNet, String fileName)
	{

	HashMap<Integer, double[]> result = new HashMap<Integer, double[]>();
	List<Instance<Integer, Integer>> data = (new GenieFileReader()).readFile(theNet, fileName, false, "\t");

	// present instances sequentially
	for (int i = 0; i < data.size(); ++i)
		{
		Instance<Integer, Integer> inst = data.get(i);
		double[] post = infer(theNet, inst.getValue()[0], inst.getValue()[1]);
		result.put(inst.getID(), post);
		}
	return result;
	}


/**
 * Read the file "happiness.txt" for test data, and evaluate it.
 * 
 * @param args	Unused.
 */
public static void main(String[] args)
	{

	smile.Network theNet = smallNet();
	System.out.printf("Network built happily.\n");

	// now do some inference

	// if I am fat and drink a lot, how likely am I to be happy?
	System.out.printf("Doing just one instance.\n");
	double[] post = infer(theNet, 0, 0);
	System.out.printf("The posterior is (%4.3f, %4.3f).\n", post[0], post[1]);

	System.out.printf("Doing a whole dataset.\n");
	Map<Integer, double[]> result = allPosteriorsFromFile(theNet, "Happiness.txt");
	double ecount0 = 0.0;
	double ecount1 = 0.0;
	for (Integer key : result.keySet())
		{
		// System.out.printf("%d : (%4.3f, %4.3f)\n",
		// key, result.get(key)[0], result.get(key)[1]);
		ecount0 += result.get(key)[0];
		ecount1 += result.get(key)[1];
		}
	System.out.printf("Expected count of Happy : %4.3f.\n", ecount0);
	System.out.printf("Expected count of Sad = %4.3f.\n", ecount1);
	}

}
