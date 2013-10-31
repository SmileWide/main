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
package smile.wide.obsolete;


import java.io.File;
import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import smile.Network;
import smile.wide.NaiveBayes;
import smile.wide.hadoop.io.DoubleArrayWritable;

/**
 * text offset 
 * the line to parse
 * the instance ID
 * the posterior
 * @author robert.e.cranfill@boeing.com
 *
 */
@Deprecated
public class InferenceMapper 
	extends Mapper<LongWritable, Text, IntWritable, DoubleArrayWritable >
{
	
	private static final Logger s_logger;
	
	static
	{
		s_logger = Logger.getLogger(InferenceMapper.class);
		s_logger.setLevel(Level.INFO);
		s_logger.debug("I live... again!");
		
		outcomeIds = new String[3][];
	}
	

	private static Network theNet;
	private static int attributesStart = 1;
	private static String[][] outcomeIds;
	private int[] nodeHandles;
	private String[] columnNames;
	
	public InferenceMapper()
	{
		s_logger.debug("Getting paths and files information");
		
		// ============== Debug
		String libpath = System.getProperty("java.library.path");
		String clspath = System.getProperty("java.class.path");
		String currentdir = System.getProperty("user.dir");
		
		s_logger.debug("Classpath is:");			// gotta print to err to see the output		
		s_logger.debug(">>>>>>" +  clspath + "<<<<<<");		
		s_logger.debug("Libpath is:");					
		s_logger.debug(">>>>>>" +  libpath + "<<<<<<");
		s_logger.debug("Current directory is: " + currentdir);		
		
		
		// list the files in the current directory		
		s_logger.debug("== Directory listing: =============================");
		File dir = new File(currentdir);
	    String[] chld = dir.list();
	    if ( chld == null)
	    {
	    	s_logger.fatal("Specified directory '" + currentdir + "' does not exist or is not a directory.");	    	
	    } 
	    else 
	    {
	    	for(int i = 0; i < chld.length; i++)
	    	{
	    		String fileName = chld[i];
	    		s_logger.debug(fileName);
	    	}
	    }
	    s_logger.debug("== Directory listing: =============================");
		
	    s_logger.debug("About to call NaiveBayes.smallNet()");
			
	 // ============== Actual init
			    
	    theNet = NaiveBayes.smallNet();
	    
	    s_logger.debug("NaiveBayes.smallNet() returns");
			
		columnNames = new String[3];					// the right way would be to use the distributed cache
		columnNames[0] = "InstID";						// to send around the .xdsl of the BN and a small metadata  
		columnNames[1] = "Drinks";						// file attached to the dataset describing the columns
		columnNames[2] = "Size";						
				
		nodeHandles = new int[3];
		nodeHandles[0] = -1;
		nodeHandles[1] = theNet.getNode(columnNames[1]);
		nodeHandles[2] = theNet.getNode(columnNames[2]);		
		
		s_logger.debug("InferenceMapper() ctor returns");
	}
		
	// implements JobConfigurable
	public void configure(JobConf job) throws IOException {
		
		System.err.println("Mapper.configure called");
		
        // Get the cached archives/files
        Path[] localArch = DistributedCache.getLocalCacheArchives(job);
        Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
        
        for (Path p : localArch)
        {
        	boolean ex = (new File(p.toString())).exists();         	
        	System.err.println("MAP TASK: Local archive " + p.toString() + " exists: " + ex); 
        }
        
        for (Path p : localFiles)
        {
        	boolean ex = (new File(p.toString())).exists();         	
        	System.err.println("MAP TASK: Local file " + p.toString() + " exists: " + ex); 
        }
        
        
        System.err.println("MAP TASK: Also, my java.library.path=" + 
        		System.getProperty("java.library.path"));
        
    }
	
		
	@Override
	public void map(LongWritable offset, Text value, Context context)
		throws IOException, InterruptedException
	{
		
		s_logger.debug(String.format("map(offset=%d    value=%s)", offset.get(), value.toString()));
		
		String line = value.toString();
		
		// =====================================================================
		// the part where we read in a line of text
		// parse the line string, check length
		String[] tokens = line.split("\t");		

		if (tokens[0].equals("ID"))
		{
			// this is the header line. Should be compared with
			// the metadata in the real version.
			return;
		}
		
		// figure out the instance id
		int id = Integer.parseInt(tokens[0]);

		Integer[] attributes = new Integer[tokens.length - attributesStart];
		for (int i=attributesStart; i < tokens.length; ++i)
		{
			// i indexes tokens
			// attrIdx indexes attributes (nodes in the network)
			// j indexes outcome values

			int attrIdx = i - attributesStart;

			// pull out the outcome IDs if we don't have them
			if (outcomeIds[attrIdx] == null) 
			{
				outcomeIds[attrIdx] = theNet.getOutcomeIds(nodeHandles[i]);	
			}

			// search linearly for the outcome named like the token
			attributes[attrIdx] = Integer.MIN_VALUE;	// initialize as invalid 
			for (int j=0; j < outcomeIds[attrIdx].length; ++j)
			{
				if (tokens[i].equals(outcomeIds[attrIdx][j]))
				{
					attributes[attrIdx] = j;
					break;
				}

				if (tokens[i].equals("*"))
				{
					// * denotes missing value
					attributes[attrIdx] = null;
					break;
				}
			}
			if (	attributes[attrIdx] != null &&  
					attributes[attrIdx].equals(Integer.MIN_VALUE))
			{
				System.err.printf("Aaaargh! Unknown attribute value '%s'\n", tokens[i]);
				System.err.printf("Valid attribute values are:\n");
				for (int j=0; j < outcomeIds[attrIdx].length; ++j)
				{
					System.err.printf("\t %s\n", outcomeIds[attrIdx][j]);
				}
			}										
		}
		
		s_logger.debug("map() - line parsed");
		
		// =========================================================================
		// the part where we run the inference
			
		for (int k=attributesStart; k < tokens.length ; ++k)
		{
			int attrIdx = k - attributesStart;
			if (attributes[attrIdx] != null)
			{
				theNet.setEvidence(theNet.getNode(columnNames[k]), attributes[attrIdx]);
			}				
		}
		
		s_logger.debug("map() - evidence set");
		
		theNet.updateBeliefs();
		
		s_logger.debug("map() - inference done");
		 	
		double[] posterior = theNet.getNodeValue(theNet.getNode("Class"));
		
		s_logger.debug("map() - posteriors pulled");
		
		// =========================================================================		
		// write to the context
		DoubleWritable[] fw = new DoubleWritable[posterior.length];
		for (int z = 0; z < posterior.length; ++z)
		{
			fw[z] = new DoubleWritable(posterior[z]);
		}			
		DoubleArrayWritable post = new DoubleArrayWritable(fw);
		context.write(new IntWritable(id), post);
		
		s_logger.debug("map() - all done");
	}
	
	
	// ===================================== Unit Test
	
	
	
	
}
