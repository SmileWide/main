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
package smile.wide.algorithms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import smile.wide.Network;
import smile.wide.data.DataSet;
import smile.wide.data.SMILEData;


import java.io.File;
import java.util.Scanner;

/** A wrapper for SMILE's BayesianSearch algorithm
 * @author m.a.dejongh@gmail.com
 */
public class BayesianSearchSMILEHadoop extends BayesianSearch {

	String ParallelRuns = new String("1");
	
	public String getParallelRuns() {
		return ParallelRuns;
	}
	public void setParallelRuns(String parallelRuns) {
		ParallelRuns = parallelRuns;
	}
	
	/** Learn function, takes general data set class downcasts to
	 * SMILE version (exception if it's not the right class).
	 * Starts a Hadoop job that runs many instances of SMILE's BayesianSearch 
	 * algorithm in parallel and returns a Network object containing
	 * a SMILE Network object which had the highest score of all attempts. 
	 * @param data general data set class to be used as input
	 * @return returns an instance of the Network class 
	 * that contains a Bayesian network.
	 * @throws Exception 
	 */	
	@Override
	Network learnStructure(DataSet data) throws Exception {
		//Here we do the Hadoop/SMILE version were we run 
		//SMILE's Bayesian Search in a number of mappers, 
		//reducing to get the best network.
		//Downcasting to SMILE dataset version (assuming this throws exception if it fails)
		//Perhaps it's better to scrap SMILEData and have a getSMILEdata function in 
		//DataSet that returns smile.DataSet() (or throws exception when fails)
		SMILEData sdata = (SMILEData) data;
		//extracting the actual dataset;
		smile.learning.DataSet d = sdata.getData();
		//probably write it to a temp file and distributed to cache.
		d.writeFile("tempsmile.txt");
		//submitting the job to hadoop (UNDER CONSTRUCTION)
		Configuration conf = new Configuration();
		String[] args = {"tempsmile.txt","/user/mdejongh/output",ParallelRuns};
		try {
			ToolRunner.run(conf, new SMILEBSjob(), args);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		//read the file we downloaded
		String output = new Scanner(new File("smile-output.txt")).useDelimiter("\\Z").next();
		//extract the SMILE XML code
		String netw = output.substring(output.indexOf("<?xml"),output.lastIndexOf('>')+1);
		//put it in a SMILE network object
		smile.Network n = new smile.Network();
		n.readString(netw);
		//put that in our Network object
		Network net = new Network();
		net.setNetwork(n);
		File f = new File("tempsmile.txt");
		f.delete();
		f = new File("smile-output.txt");
		f.delete();
		return net;
	}
	/** main function, executes the job on the cluster*/
	public static void main(String[] args) throws Exception {
		BayesianSearchSMILEHadoop test = new BayesianSearchSMILEHadoop();
		test.setParallelRuns(args[1]);
		SMILEData d = new SMILEData();
		d.Read(args[0]);
		Network net = test.learnStructure(d);
		net.writeFile("output.xdsl");
		System.exit(0);
	}

}
