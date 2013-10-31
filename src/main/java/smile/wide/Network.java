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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import smile.wide.BNQuery;
import smile.wide.InferenceDriver;
import smile.wide.data.DataSet;
import smile.wide.data.DataSetReader;
import smile.wide.hadoop.io.DoubleArrayWritable;

/**
 * The main class of SMILE-WIDE. Forwards most (construction) calls to an embedded
 * instance of smile.Network, but adds the infer() method which executes
 * on Hadoop infrastructure.
 * 
 * @author robert.e.cranfill@boeing.com
 * @author tomas.singliar@boeing.com
 * 
 * @see  <a href="http://genie.sis.pitt.edu/wiki/SMILE.NET:_Network" target="blank">SMILE online documentation</a>
 * 
 */
public class Network implements NetworkIF
{
private smile.Network hiddenNetwork_;
private Logger logger_;

// Hadoop execution
private Configuration conf_ = null;
private Path jobHDFSPath_ = null; // the HDFS path of the last Hadoop job
private Path tempDir_ = null;

// intermediate result
private int[] instanceIDs_ = null;
private float[][] posteriors_ = null; // each row corresponds to an instance


// each column to a query variable's possible value

// ========================================================================
// Setting the network up - calls forwarded to jsmile.Network

public Network()
	{
	logger_ = Logger.getLogger(Network.class);
	logger_.setLevel(Level.DEBUG);

	hiddenNetwork_ = new smile.Network();
	}


public Network(Logger logger)
	{
	logger_ = logger;
	hiddenNetwork_ = new smile.Network();
	}


public Network(String networkFileName)
	{
	logger_ = Logger.getLogger(Network.class);
	logger_.setLevel(Level.DEBUG);
	hiddenNetwork_ = new smile.Network();
	hiddenNetwork_.readFile(networkFileName);
	}


// ========================================================================
// Setting the network up - calls forwarded to jsmile.Network

@Override
// smilewide.NetworkIF
public void addOutcome(int nodeHandle, String outcomeId)
	{
	logger_.debug("smilewide.addOutcome(int nodeHandle, String outcomeId)");
	hiddenNetwork_.addOutcome(nodeHandle, outcomeId);
	}


public void addOutcome(String nodeId, String outcomeId)
	{
	logger_.debug("smilewide.addOutcome(String nodeId, String outcomeId)");
	hiddenNetwork_.addOutcome(nodeId, outcomeId);
	}


@Override
// smilewide.NetworkIF
public int addNode(int cpt, String string)
	{
	logger_.debug("smilewide.addNode");
	return hiddenNetwork_.addNode(cpt, string);
	}


@Override
// smilewide.NetworkIF
public void deleteOutcome(int nodeHandle, int outcomeIndex)
	{
	logger_.debug("smilewide.deleteOutcome(int nodeHandle, int outcomeIndex)");
	hiddenNetwork_.deleteOutcome(nodeHandle, outcomeIndex);
	}


@Override
// smilewide.NetworkIF
public void deleteOutcome(String nodeId, String outcomeId)
	{
	logger_.debug("smilewide.deleteOutcome(String nodeId, String outcomeId)");
	hiddenNetwork_.deleteOutcome(nodeId, outcomeId);
	}


@Override
// smilewide.NetworkIF
public void setNodeDefinition(int nodeHandle, double[] definition)
	{
	logger_.debug("smilewide.setNodeDefinition(int nodeHandle, double[] definition)");
	hiddenNetwork_.setNodeDefinition(nodeHandle, definition);
	}


@Override
// smilewide.NetworkIF
public void setNodeDefinition(String nodeId, double[] definition)
	{
	logger_.debug("smilewide.setNodeDefinition(String nodeId, double[] definition)");
	hiddenNetwork_.setNodeDefinition(nodeId, definition);
	}


@Override
// smilewide.NetworkIF
public void setOutcomeId(int nodeHandle, int outcomeIndex, String id)
	{
	logger_.debug("smilewide.setOutcomeId");
	hiddenNetwork_.setOutcomeId(nodeHandle, outcomeIndex, id);
	}


@Override
// smilewide.NetworkIF
public void updateBeliefs()
	{
	logger_.debug("smilewide.updateBeliefs");
	hiddenNetwork_.updateBeliefs();
	}


@Override
// smilewide.NetworkIF
public int getNode(String nodeId)
	{
	logger_.debug("smilewide.getNode");
	return hiddenNetwork_.getNode(nodeId);
	}


@Override
// smilewide.NetworkIF
public void addArc(int parentHandle, int childHandle)
	{
	logger_.debug("smilewide.addArc");
	hiddenNetwork_.addArc(parentHandle, childHandle);
	}


@Override
// smilewide.NetworkIF
public void setEvidence(int nodeHandle, int outcomeIndex)
	{
	logger_.debug("smilewide.setEvidence");
	hiddenNetwork_.setEvidence(nodeHandle, outcomeIndex);
	}


@Override
// smilewide.NetworkIF
public double[] getNodeDefinition(String nodeId)
	{
	logger_.debug("getNodeDefinition");
	return hiddenNetwork_.getNodeDefinition(nodeId);
	}


@Override
// smilewide.NetworkIF
public double[] getNodeValue(int nodeHandle)
	{
	logger_.debug("smilewide.getNodeValue");
	return hiddenNetwork_.getNodeValue(nodeHandle);
	}


@Override
// smilewide.NetworkIF
public double[] getNodeValue(String nodeID)
	{
	logger_.debug("smilewide.getNodeValue");
	return hiddenNetwork_.getNodeValue(nodeID);
	}


@Override
// smilewide.NetworkIF
public void clearAllEvidence()
	{
	logger_.debug("smilewide.clearAllEvidence");
	hiddenNetwork_.clearAllEvidence();
	}


@Override
// smilewide.NetworkIF
public int getNodeCount()
	{
	logger_.debug("smilewide.getNodeCount");
	return hiddenNetwork_.getNodeCount();
	}


@Override
// smilewide.NetworkIF
public int getOutcomeCount(String nodeId)
	{
	logger_.debug("getOutcomeCount");
	return hiddenNetwork_.getOutcomeCount(nodeId);
	}


@Override
// smilewide.NetworkIF
public String[] getOutcomeIds(int nodeHandle)
	{
	logger_.debug("smilewide.getOutcomeIds");
	return hiddenNetwork_.getOutcomeIds(nodeHandle);
	}


@Override
// smilewide.NetworkIF
public String[] getOutcomeIds(String nodeId)
	{
	logger_.debug("smilewide.getOutcomeIds");
	return hiddenNetwork_.getOutcomeIds(nodeId);
	}


@Override
// smilewide.NetworkIF
public void readFile(String fileName)
	{
	logger_.debug("smilewide.readFile");
	hiddenNetwork_.readFile(fileName);
	}


@Override
// smilewide.NetworkIF
public void setEvidence(int nodeHandle, String outcomeId)
	{
	logger_.debug("smilewide.setEvidence");
	hiddenNetwork_.setEvidence(nodeHandle, outcomeId);
	}


@Override
// smilewide.NetworkIF
public void writeFile(String fileName)
	{
	logger_.debug("smilewide.writeFile");
	hiddenNetwork_.writeFile(fileName);
	}


// ========================================================================================
// Accessors

/**
 * Set the SMILE network using an XSDL-formatted string.
 * 
 * @param xdsl	The string representation of the network.
 */
public void setNetwork(String xdsl)
	{
	logger_.debug("smilewide.setNetwork");
	hiddenNetwork_.readString(xdsl);
	}


/**
 * Get the SMILE network as an XDSL-formatted string.
 * 
 * @return net The string representation of the network.
 */
public String getNetworkString()
	{
	return hiddenNetwork_.writeString();
	}


/**
 * Set the underlying smile.Network.
 * 
 * @param network	The network.
 */
public void setNetwork(smile.Network network)
	{
	hiddenNetwork_ = network;
	}


/**
 * Get the underlying smile.Network.
 * 
 * @return A smile.Network object.
 */
public smile.Network getNetwork()
	{
	return hiddenNetwork_;
	}


// ========================================================================================
// BigData calls

/**
 * Runs inference in parallel on a large dataset. The result is stored as a two dimensional array -
 * major dimension corresponding to the instance, and minor to the query variable value.
 * 
 * Result can be retrieved by calling inferenceResult().
 * 
 * Preconditions for correct use:
 * - all variables in the evidence set are present in the BN
 * - the query variable is present in the BN
 * - the names of all variables in the evidence set are names of columns in the DataSet
 * (if the variable is not in the evidence, why condition on it?)
 * 
 * 
 * @param dataset	The dataset to run inference on
 * @param q			The query to ask of each instance
 */
public void infer(DataSet dataset, DataSetReader<?, ?> reader, BNQuery q)
	{
	InferenceDriver id = new InferenceDriver();
	conf_ = id.getConf(); // get the inference driver's config and set it up

	// find out the working location
	try
		{
		Job j = new Job(conf_);
		jobHDFSPath_ = j.getWorkingDirectory();
		}
	catch (IOException e1)
		{
		e1.printStackTrace();
		return;
		}

	int r = (new Random()).nextInt();
	tempDir_ = new Path(jobHDFSPath_ + "/tmp/infresult_" + r);

	// write the network somewhere into HDFS - relies on two subsequent jobs
	// starting in the same directory, usually user home
	// TODO: communicate the actual location to the subsequent job?
	String name = hiddenNetwork_.getName() + ".xdsl";
	try
		{

		hiddenNetwork_.writeFile("/tmp/" + name);
		FileSystem fs = FileSystem.get(conf_);
		fs.mkdirs(new Path(jobHDFSPath_ + "/tmp/"));
		fs.moveFromLocalFile(new Path(name), new Path(jobHDFSPath_ + "/tmp/" + name));
		}
	catch (IOException e)
		{
		logger_.error("I/O Error recording the Bayes network " + name + " to " + jobHDFSPath_ + "/tmp/" + name);
		e.printStackTrace();
		}
	conf_.set("xdata.bayesnets.networkfile", jobHDFSPath_ + "/tmp/" + name);

	// tell the driver the reader class
	conf_.set("xdata.bayesnets.datasetreader.class", reader.getClass().getName());

	// pull out the column indices that correspond to the query variables
	// DataSet should know which columns are which

	ArrayList<String> evVars = q.getEvidenceVars();
	String qvar = q.getQueryVar();
	int[] colIndices = new int[evVars.size() + 1];
	String[] colNames = new String[evVars.size() + 1];

	colIndices[0] = dataset.indexOfColumn(qvar);

	for (int i = 1; i <= evVars.size(); ++i)
		{
		colIndices[i] = dataset.indexOfColumn(evVars.get(i - 1));
		colNames[i] = evVars.get(i - 1);
		}
	Arrays.sort(colIndices);

	conf_.set("xdata.bayesnets.datasetreader.filter", concat(colIndices, ","));

	// name the variables to which the dataset columns map
	conf_.set("xdata.bayesnets.datasetreader.variablenames", concat(colNames, ","));

	int instID = dataset.instanceIDColumnIndex();
	if (instID == -1)
		{
		logger_.error("No instance ID column index in dataset " + dataset.getName()
				+ ". Dataset must provide one for inference.");
		return;
		}
	conf_.set("xdata.bayesnets.datasetreader.instid", "" + instID);

	conf_.set("xdata.bayesnets.queryvariable", q.getQueryVar());

	// arguments for the inference driver are the location of the dataset and where
	// to write
	String[] args = new String[2]; // 2 arguments
	args[0] = dataset.location().toString();
	args[1] = tempDir_.toString();

	try
		{
		id.setConf(conf_); // make sure we are not setting up a side copy of the conf...
		ToolRunner.run(id, args);
		}
	catch (Exception e)
		{
		logger_.error("Something went wrong in executing the inference job");
		e.printStackTrace();
		}

	}


/**
 * Retrieve the result from file and be ready to return it as an array
 * from inferenceResult()
 */
public void retrieveResult()
	{

	FileSystem fs;
	try
		{
		fs = FileSystem.get(conf_);

		FileStatus[] stats = fs.listStatus(tempDir_);

		SequenceFile.Reader reader = null;
		ArrayList<Integer> instids = new ArrayList<Integer>();
		ArrayList<float[]> posts = new ArrayList<float[]>();

		// read all output files
		for (FileStatus stat : stats)
			{
			if (stat.getPath().toUri().toString().contains("part-r-"))
				try
					{
					logger_.info("Reading results from " + stat.getPath());
					reader = new SequenceFile.Reader(fs, stat.getPath(), conf_);
					IntWritable key = new IntWritable();
					DoubleArrayWritable value = new DoubleArrayWritable();

					while (reader.next(key, value))
						{
						instids.add(key.get());
						DoubleWritable[] unpack = (DoubleWritable[]) value.get();
						float[] post = new float[unpack.length];
						for (int i = 0; i < unpack.length; ++i)
							{
							post[i] = (float) unpack[i].get();
							}
						posts.add(post);
						}
					}
				finally
					{
					IOUtils.closeStream(reader);
					}
			}

		// copy over the result
		assert (instids.size() == posts.size());
		instanceIDs_ = new int[instids.size()];
		posteriors_ = new float[posts.size()][];
		for (int i = 0; i < instids.size(); ++i)
			{
			instanceIDs_[i] = instids.get(i);
			posteriors_[i] = posts.get(i);
			}

		}
	catch (IOException e)
		{
		logger_.error("Could not read in the inference results");
		e.printStackTrace();
		}

	}


public int[] getInstanceIDs()
	{
	return instanceIDs_;
	}


public float[][] inferenceResult()
	{
	return posteriors_;
	}


// ================================================================================
// Little helpers

/**
 * Concatenate values using the given delimiter
 * 
 * @param colIndices
 * @param delim
 * @return
 */
private String concat(int[] colIndices, String delim)
	{

	String result = "";
	for (int i = 0; i < colIndices.length - 1; ++i)
		{
		result += colIndices[i] + delim;
		}

	if (colIndices.length > 0)
		{
		result += colIndices[colIndices.length - 1];
		}

	return result;
	}


/**
 * Concatenate values using the given delimiter
 * 
 * @param colIndices
 * @param delim
 * @return
 */
private <T> String concat(T[] colIndices, String delim)
	{

	String result = "";
	for (int i = 0; i < colIndices.length - 1; ++i)
		{
		result += colIndices[i] + delim;
		}

	if (colIndices.length > 0)
		{
		result += colIndices[colIndices.length - 1];
		}

	return result;
	}

}
