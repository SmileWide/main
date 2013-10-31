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

import java.net.URI;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/** 
 * This MR job collects all values of all attributes in a dataset.
 * 
 *  It expects as arguments:
 *  <ul>
 *  <li> input path
 *  <li> output path
 *  <li> xdata.bayesnets.datasetreader.class=[class]>
 *    name of a "SerDe" class that can turn a row into an Instance,
 *    should implement DataSetReader
 *    
 *	<li> xdata.bayesnets.datasetreader.filter=[filter string]
 *	  the filter - which columns of data are being pulled out
 * 
 *	<li> xdata.bayesnets.datasetreader.instid=[number > 0]
 *	  which column is the instance ID
 *  </ul>
 * @author tomas.singliar@boeing.com
 *
 */
public class AttributeValueHistogram
	extends Configured implements Tool
{
	// =================================================================
	// statics
	private static final Logger s_logger;
	
	static
	{
		s_logger = Logger.getLogger(AttributeValueHistogram.class);
		s_logger.setLevel(Level.DEBUG);	
	}
	
	// =================================================================
	// instance members
	
	// to store the values grabbed from the file
	private TreeMap<String, Map<String, Integer>> attributeValues_ = null;
	
	// arguments
	private String readerClass_ = null;
	private String readerFilter_ = null;
	private String readerInstID_ = null;
	private String variableNames_ = null;
	private String inPath_ = null; 	
	
	private Configuration conf_;
	private URI lastWorkingDir_;
	
	// =================================================================
	// methods
	
	@Override
	public int run(String[] arg) throws Exception {
								
		if (arg.length < 2)
		{
			s_logger.fatal("Usage: AttributeValueHistogram <infile> <outfile>");
			// TODO: return an error code?
		}
		
		s_logger.debug("Got " + arg.length + " arguments");
		
		inPath_ = arg[0];
		s_logger.info("Input path is " + inPath_);
		
		// parse the key-value arguments passed - by now these are the arguments
		// specific to AttributeValueHistogram
		for (int i = 1; i < arg.length; ++i)
		{
			String[] tokens = arg[i].split("=");
			if (tokens.length != 2) 
			{
				s_logger.fatal("Can't parse argument" + arg[i]);
			}
			
			if (tokens[0].equals("xdata.bayesnets.datasetreader.class"))
			{
				readerClass_ = tokens[1].trim();
				s_logger.debug("Set reader class to " + readerClass_);
			}
			else if (tokens[0].equals("xdata.bayesnets.datasetreader.filter"))
			{
				readerFilter_ = tokens[1].trim(); 
				s_logger.debug("Set reader filter to " + readerFilter_);
			}
			else if (tokens[0].equals("xdata.bayesnets.datasetreader.instid"))
			{
				readerInstID_ = tokens[1].trim();
				s_logger.debug("Set reader's instance ID column to " + readerInstID_);
			}
			else if (tokens[0].equals("xdata.bayesnets.datasetreader.variablenames"))
			{
				variableNames_ = tokens[1].trim();
				s_logger.debug("Set reader's variable names to " + variableNames_);
			}
			else
			{
				s_logger.warn("Unknown argument " + arg[i]);
			}
		}		
		
		conf_ = getConf();
		
		// pass the reader class to the mapper, in jobconf		
		// TODO: use setClass here - fails early if wrong, not in the mapper
		conf_.set("xdata.bayesnets.datasetreader.class", readerClass_);		
		conf_.set("xdata.bayesnets.datasetreader.filter", readerFilter_);		
		// conf_.set("xdata.bayesnets.datasetreader.instid", readerInstID_); // not used
		conf_.set("xdata.bayesnets.datasetreader.variablenames", variableNames_);
				
		conf_.setBoolean("mapred.compress.map.output", true);							// compress intermediate data
		conf_.set("mapred.output.compression.type", CompressionType.BLOCK.toString()); 	// by block, to keep splittable
		conf_.setClass("mapred.map.output.compression.codec", GzipCodec.class, CompressionCodec.class);
		
		// for debugging					
		conf_.set("keep.failed.task.files","true");			
		conf_.set("keep.failed.task.pattern","*");
		
		Job job = new Job(conf_);
		
		job.setJarByClass(AttributeValueHistogram.class);	// use this jar
		job.setJobName("Collect value histograms by attribute");
											
		FileInputFormat.addInputPath(job, new Path(inPath_));
		
		int rnd = (new Random()).nextInt();		
		lastWorkingDir_ = job.getWorkingDirectory().toUri();
		s_logger.info("Job working directory is " + lastWorkingDir_);
		String tempDirName = job.getWorkingDirectory() + "/tmp/attvalhist" + rnd + ".tmp";
		s_logger.info("Temp files in directory " + tempDirName);
		FileOutputFormat.setOutputPath(job, new Path(tempDirName));
		
		job.setMapperClass(AttributeValueHistogramMapper.class);
		job.setCombinerClass(AttributeValueHistogramReducer.class);
		job.setReducerClass(AttributeValueHistogramReducer.class);
		
		// set both the map and reduce in/out classes
		job.setOutputKeyClass(Text.class);				// the name of the attribute
		job.setOutputValueClass(MapWritable.class);		// Value -> count map
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
				
		// run'em
		int result = job.waitForCompletion(true) ? 0 : 16;
		
		// retain the temp file, collect the output		
		attributeValues_ = new TreeMap<String, Map<String, Integer>>(); 		
	
		FileSystem fs = FileSystem.get(conf_);			 
		SequenceFile.Reader reader = null;
		
		Path resPath = new Path(tempDirName);
		FileStatus[] stats = fs.listStatus(resPath);

		// read all output files
		for (FileStatus stat : stats)
		{			
			if (stat.getPath().toUri().toString().contains("part-r-"))
			try {			
				s_logger.info("Reading results from " + stat.getPath());
				reader = new SequenceFile.Reader(fs, stat.getPath(), conf_);
				// Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf_);
				// MapWritable value = (MapWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf_);
				Text key = new Text();
				MapWritable value = new MapWritable();		

				while (reader.next(key,value))
				{
					TreeMap<String, Integer> valueCounts = new TreeMap<String, Integer>();
					for (Writable attValue : value.keySet())
					{
						valueCounts.put(((Text) attValue).toString(), 
								((IntWritable)(value.get(attValue))).get());
					}				
					attributeValues_.put(key.toString(), valueCounts);
				}
			}
			finally 
			{
				IOUtils.closeStream(reader);
			}
		}

		fs.deleteOnExit(resPath);
		
		return result;
	}
 
	public SortedMap<String, Map<String, Integer>> AttributeValues() {		
		return attributeValues_;
	}

	public URI getWorkingDir() {// TODO Auto-generated method stub
		return lastWorkingDir_;
	}

}
