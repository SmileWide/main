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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import smile.Network;
import smile.wide.data.DataSetReader;
import smile.wide.data.Instance;
import smile.wide.hadoop.io.DoubleArrayWritable;

/**
 * <p>
 * Extends Hadoop's Mapper class:
 * </p><p>
 * Mapper&lt;LongWritable, Text, LongWritable, DoubleArrayWritable>
 * </p><ul>
 * <li>LongWritable: text offset</li>
 * <li>Text: the line to parse</li>
 * <li>LongWritable: the instance ID</li>
 * <li>DoubleArrayWritable: the posterior</li>
 * </ul>
 * @author tomas.singliar@boeing.com
 */
public class PerInstanceInferenceMapper
	extends Mapper<LongWritable, Text, LongWritable, DoubleArrayWritable>
{
private static final Logger s_logger;

static
{
	s_logger = Logger.getLogger(PerInstanceInferenceMapper.class);
	s_logger.setLevel(Level.INFO);	
}

//===================================================================
// Instance members

private Configuration conf_ = null;
private String networkFile_ = null;	
private String fileReaderClass_ = null;		// what class to use to read the file 
private String fileReaderFilter_ = null;	// a filter - which columns to use
private int instanceIDcolumn_ = -1;	// which one of them is the instance ID
private String queryVariable_ = null;
private String[] dataVariables_ = null; 

private Network theNet_ = null;
private DataSetReader<Long,String> reader_;

private boolean initializing_ = true;

private HashMap<Integer, HashSet<String>> validOutcomes_ = new HashMap<Integer, HashSet<String>>();

// ===================================================================

public PerInstanceInferenceMapper()
{
	
}

@SuppressWarnings("unchecked")
@Override
public void map(LongWritable offset, Text value, Context context)
	throws IOException, InterruptedException
{
	// initialize in  the first map call	
	if (initializing_)
	{
		// get the information needed
		conf_ = context.getConfiguration();
		networkFile_ = conf_.get("xdata.bayesnets.networkfile");
		fileReaderClass_ = conf_.get("xdata.bayesnets.datasetreader.class");
		fileReaderFilter_ = conf_.get("xdata.bayesnets.datasetreader.filter");
		instanceIDcolumn_ = Integer.parseInt(conf_.get("xdata.bayesnets.datasetreader.instid"));
		queryVariable_ = conf_.get("xdata.bayesnets.queryvariable");
		dataVariables_ = conf_.get("xdata.bayesnets.datasetreader.variablenames").split(",");						 				

		// initialize the Bayes net
		theNet_ = new Network();
		theNet_.readFile(networkFile_);
		
		// initialize the file reader
		try {
			Object r = Class.forName(fileReaderClass_).newInstance();			
			reader_  = (DataSetReader<Long,String>) r;  
		} catch (InstantiationException e) {
			s_logger.error("Instantiation exception for DataSetReader " + fileReaderClass_);
			e.printStackTrace();
			System.exit(1);
		} catch (IllegalAccessException e) {
			s_logger.error("IllegalAccess exception for DataSetReader " + fileReaderClass_);
			e.printStackTrace();
			System.exit(1);
		} catch (ClassNotFoundException e) {			 
			s_logger.error("ClassDefNotFoundException for DataSetReader " + fileReaderClass_);
			e.printStackTrace();
			System.exit(1);
		} catch (ClassCastException e) {
			s_logger.error("ClassCastException for DataSetReader " + fileReaderClass_);
			e.printStackTrace();
			System.exit(1);
		}		
		reader_.setFilter(fileReaderFilter_);
		reader_.setInstanceIDColumn(instanceIDcolumn_);
						
		initializing_ = false;
	}

	
	// here starts the part where we are already initialized and going line-by-line
	
	Instance<Long,String> inst = reader_.parseLine(value.toString());

	// =========================================================================
	// the part where we run the inference

	theNet_.clearAllEvidence();
	// instantiate evidence
	for (int i=0; i < inst.getValue().length; ++i)
	{		
		int nodeId = theNet_.getNode(dataVariables_[i]);			// TODO: unnecessary JNI crossing
		String ev = inst.getValue()[i];
		if (isValidOutcomeID(ev, nodeId))				
		{
			theNet_.setEvidence(nodeId, ev);	// TODO: could be something else then String ...
		}
	}

	s_logger.debug("map() - evidence set");

	theNet_.updateBeliefs();

	s_logger.debug("map() - inference done");

	double[] posterior = theNet_.getNodeValue(theNet_.getNode(queryVariable_));

	s_logger.debug("map() - posteriors pulled");

	// =========================================================================		
	// write to the context
	DoubleWritable[] fw = new DoubleWritable[posterior.length];
	for (int z = 0; z < posterior.length; ++z)
	{
		fw[z] = new DoubleWritable(posterior[z]);
	}			
	DoubleArrayWritable post = new DoubleArrayWritable(fw);
	LongWritable key = new LongWritable(inst.getID());
	context.write(key, post);

	s_logger.debug("map() - all done");
}

private boolean isValidOutcomeID(String ev, int forNode) {
		
	// assuming the forNode input is a valid nodeId 
	
	if (validOutcomes_.containsKey(forNode))
	{
		// we have retrieved this node before
		if (validOutcomes_.get(forNode).contains(ev))
		{
			return true;
		}		
		return false;
	}
		
	// otherwise retrieve the valied outcomes
	String[] ids = theNet_.getOutcomeIds(forNode);
	HashSet<String> alloutcomes = new HashSet<String>();
	for (String s : ids)
		alloutcomes.add(s);	
	validOutcomes_.put(forNode, alloutcomes);
	
	// is it valid now?
	return isValidOutcomeID(ev, forNode);
}

}
