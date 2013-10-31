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
package smile.wide.facebook;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import smile.Network;
import smile.wide.AttributeValueHistogram;
import smile.wide.PerInstanceInferenceMapper;
import smile.wide.hadoop.io.DoubleArrayWritable;


/** The experimental driver class for the facebook data test 
 * 
 * This class should drive more generalized inference mapper 
 * and reducer classes. In general, experiment drivers should
 * - run a preparation step in which the necessary information   
 * is collected about the datasets, 
 * - the network is built up using this information
 * - saved in a file
 * - run inference or learning as needed on the saved network
 *  
 * This class is also scheduled to become obsolete as it is  
 * replaced by the general Network class. Ideally, the user 
 * should not have to write long drivers like this.
 *  
 * @author tomas.singliar@boeing.com
 *
 */
public class ExperimentDriver 		
	extends Configured implements Tool
{	
	// =======================================================================
	// Parameters
	
	// parameterizing the Hive connection
	private static boolean useHiveJDBC_ = false;
	
	// TODO: make this HiveJDBC configuration a collection of command-line parameters
	private static String jdbcDriverName = "org.apache.hadoop.hive.jdbc.HiveDriver";	
	private static String jdbcConnectURL = "jdbc:hive://centoshead:10000/default";
	private static String tableName = "facebook";
	
	// HDFS directory where the native lib lives - will be taken from there by distributed cache
	// TODO: make this a configurable parameter
	private String libHDFSPath_ = "/user/tsingliar/lib/linux64";
	
	// TODO: make this a command-line configurable parameter
	String[] columns_ = new String[] {"FirstName", "MiddleName","Sex", "IsAppUser", "FriendCount", "LikesCount"};
	
	// =======================================================================
	// Logging
	private static final Logger s_logger;
	static
	{
		s_logger = Logger.getLogger(ExperimentDriver.class);
		s_logger.setLevel(Level.DEBUG);
	}

	// =======================================================================
	// Parameters obtained from command line or execution environment
	
	private String inPath_ = null;
	private String outPath_ = null;
	private URI jobHDFSPath_ = null;
	
	// file which will have the Bayesian network which
	// will be processed by the inference phase
	private String networkFile_ = null; 
	private String modifiedNetwork_ = null;
	private Configuration conf_ = getConf();	

	// =======================================================================
	// Stuff from data  

	// stuff that comes from the preparation stage to the inference stage
	private Map<String, Map<String, Integer>> attValueCounts_;
	
	
	// ================================================================================
	// Methods
	
	/** Populates attValueCounts_ by executing a Hive query and parsing the result
	 * 
	 * @return 0 if success
	 */
	private int collectAttValuesFromHive() throws SQLException, FileNotFoundException
	{
		attValueCounts_ = new HashMap<String, Map<String, Integer>>();
		
		try {				
			Class.forName(jdbcDriverName);
		} catch (ClassNotFoundException e) {
			s_logger.error("Can't inititialize the HiveJDBC driver");
			e.printStackTrace();				
			return 1;
		}
		
		Connection con = DriverManager.getConnection(jdbcConnectURL, "", "");
		Statement stmt = con.createStatement();				
		// show tables
		
		String sql = "use facebook"; 
		stmt.executeQuery(sql);
		sql = "describe " + tableName;
		System.out.println("Table description: " + sql);
		ResultSet res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}						
		
		// for each column, record unique values
		// suboptimal to do it one by one, but for testing hive connectivity will do
		for (String column : columns_)
		{
			Map<String,Integer> valueCounts = new HashMap<String,Integer>();
			sql = "select " + column + ", count(*) from facebook group by " + column;
			res = stmt.executeQuery(sql);			
			while (res.next()) {
				String value = res.getString(1);
				System.out.println("Attribute " + column); 
				System.out.println("\t " + value + "\t " + res.getInt(2) + " occurences");
				valueCounts.put(value, res.getInt(2));
			}
			attValueCounts_.put(column, valueCounts);
		}
					
		return 0;
	}
	
	/** Populates attValueCounts_ by running an MR Job
	 *  Populates jobHDFSPath_ with someplace we can put temp files.
	 * 
	 * @return 0 if success
	 */
	private int collectAttValuesByMRJob()
	{
		// pathInfo();
		
		// go through the dataset and collect all values of all columns
		AttributeValueHistogram coll = new AttributeValueHistogram();
					
		// let's pass this through the args interface, just as an exercise
		// in doing it this way because AttributeValueHistogram could be a 
		// standalone job, too, so it needs to be able to parse happily
		// conf_.set("xdata.bayesnets.datasetreader.class", FacebookCSVReader.class.getName());
		// conf_.set("xdata.bayesnets.datasetreader.filter", "3,5,7,10,11,12" );
								
		String[] args = new String [] {
							inPath_, 
							"xdata.bayesnets.datasetreader.class" + "=" + FacebookCSVReader.class.getName(), 
							"xdata.bayesnets.datasetreader.filter" + "=" + "3,5,7,10,11,12",
							"xdata.bayesnets.datasetreader.instid" + "=" + "1",
							"xdata.bayesnets.datasetreader.variablenames" + "=" + concat(columns_,",")};
		try {				
			ToolRunner.run(conf_, coll, args);
			jobHDFSPath_ = coll.getWorkingDir();
			attValueCounts_ = coll.AttributeValues();
		}
		catch (Exception e)
		{
			s_logger.error("Exception caught in AtributeValueHistogram: " + e.getMessage());
			e.printStackTrace();
			return 2;
		}
		
		return 0;
	}		
	
	/**
	 * Runs preparation step for the experiment:
	 * collects attribute values and prepares the bayes network  
	 * 
	 * @return error code from execution
	 * @throws SQLException
	 * @throws FileNotFoundException
	 */
	private int prepare() 
	{				
		// collect the values contained in the dataset
		if (useHiveJDBC_)
		{						
			try {
				collectAttValuesFromHive();
			} catch (FileNotFoundException e) {
				s_logger.error("Hive attribute/value collection failed on file error");
				e.printStackTrace();
			} catch (SQLException e) {
				s_logger.error("Hive attribute/value collection failed on SQL problem.");
				e.printStackTrace();
			}			
		}
		else	
		{
			collectAttValuesByMRJob();
		}
			
		// load the network file
		Network theNet = new Network();
		theNet.readFile(networkFile_);			
		
		// modify it using the collected attributes
		// update the possible outcomes for every node
		for (String var : new String[] {"FirstName", "MiddleName", "Sex", "IsAppUser"})
		{	
			String[] origOutcomes = theNet.getOutcomeIds(var);

			s_logger.debug("In the data, " + attValueCounts_.get(var).size() + " outcomes for " + var);
			
			HashSet<String> normalizedValues = new HashSet<String>(); 
			
			for (String attval : attValueCounts_.get(var).keySet())
			{
				String trimmed = attval.replace(".","").trim().toLowerCase();				
				if (trimmed.matches("[a-zA-Z_]\\w*")) 										
				{					
					normalizedValues.add(trimmed);											
				}
			}
			
			s_logger.debug(		"Normalized " + attValueCounts_.get(var).size() 
							+ 	" outcomes for " + var + " to " + normalizedValues.size());
			
			int i=0;
			for (String av : normalizedValues)
			{
				theNet.addOutcome(var, av);
				++i;
				if (i % 10000 == 0)
				{
					s_logger.info("Adding attribute # " + i + " - " + av);
				}
					
			}
			
			s_logger.debug("Deleting the original outcomes from " + var);
			for (String out : origOutcomes)
			{
				if (!normalizedValues.contains(out) && (theNet.getOutcomeCount(var) > 2))
				{
					theNet.deleteOutcome(var, out);
				}
			}
			
			s_logger.debug("Setting non-zero CPT for " + var);
			// make all outcomes possible
			double [] def = theNet.getNodeDefinition(var);
			int outcomes = theNet.getOutcomeCount(var);
			int parentconf = def.length / outcomes; // how many parent configurations
			Random r = new Random();			
			for (int p = 0; p < parentconf; ++p)
			{
				double sum = 0.0;
				double[] par = new double[outcomes];
				for (int j=0; j < outcomes; ++j)
				{
					par[j] = r.nextDouble() + 0.01;
					sum += par[j];
				}
				for (int j=0; j < outcomes; ++j)
				{
					def[p * outcomes + j] = par[j] / sum;
				}							
			}
			theNet.setNodeDefinition(var, def);
		}

		// save a modified copy
		modifiedNetwork_ = basename(networkFile_);
		modifiedNetwork_ = modifiedNetwork_.substring(0, modifiedNetwork_.lastIndexOf("."))  + ".mod.xdsl";
		try {
			theNet.writeFile(modifiedNetwork_);		 
			FileSystem fs = FileSystem.get(conf_);
			fs.mkdirs(new Path(jobHDFSPath_ + "/tmp/"));
			fs.moveFromLocalFile(new Path(modifiedNetwork_),
								 new Path(jobHDFSPath_ + "/tmp/" + modifiedNetwork_));			
		} catch (IOException e) {
			s_logger.error("I/O Error recording the modified Bayes network " 
							+ modifiedNetwork_ + " to " 
							+ jobHDFSPath_ + "/tmp/" + modifiedNetwork_);
			e.printStackTrace();
		}
				
		return 0;
	}

	/**
	 * Used for debugging path issues.
	 */
	@SuppressWarnings("unused")	
	private void pathInfo()
	{
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
	}

	/** Concatenate strings using the give delimiter
	 * 
	 * @param strings
	 * @param delim
	 * @return
	 */
	private String concat(String[] strings, String delim) {

		String result = ""; 
		for (int i=0; i < strings.length - 1 ; ++ i)
		{
			result += strings[i] + delim;
		}
		
		if (strings.length > 0)
		{
			result += strings[strings.length-1]; 
		}
		
		return result;
	}

	private int inference()
	{
		try	
		{	
			DistributedCache.createSymlink(conf_);		
			try {			
				DistributedCache.addCacheFile(					
						new URI(libHDFSPath_ + "/smile.jar#smile.jar"), conf_);			
				DistributedCache.addCacheFile(
						new URI(libHDFSPath_ + "/libjsmile.so#libjsmile.so"), conf_);
				DistributedCache.addCacheFile(
						new URI(jobHDFSPath_ + "/tmp/" + modifiedNetwork_ + "#"  + basename(modifiedNetwork_)), conf_);
			} catch (URISyntaxException e) {
				s_logger.fatal("Bad URL for modifed network file.");
				return -12;				
			}
						
			// the principle for whether to use string column names or integer column indexes:
			// - when talking about variables in the BN, use string names
			// - when talking about data munging, use column indexes.
			
			// configure the inference task
			conf_.set("xdata.bayesnets.networkfile", basename(modifiedNetwork_));
			conf_.set("xdata.bayesnets.datasetreader.class", FacebookCSVReader.class.getName());
			conf_.set("xdata.bayesnets.datasetreader.filter", "3,5,7,10,11,12" );
			conf_.set("xdata.bayesnets.datasetreader.variablenames", 
					"FirstName,MiddleName,Sex,IsAppUser,LikesCount,FriendCount" );
			conf_.set("xdata.bayesnets.datasetreader.instid", "1");
			conf_.set("xdata.bayesnets.queryvariable", "Age");			
			
			Job job = new Job(conf_);
			
			job.setJarByClass(ExperimentDriver.class);	// use this jar
			job.setJobName("Facebook Inference Performance Test");
			
			FileInputFormat.addInputPath(job, new Path(inPath_));
			FileOutputFormat.setOutputPath(job, new Path(outPath_));
			
			job.setMapperClass(PerInstanceInferenceMapper.class);
			// there need not be any reducer
			// job.setReducerClass(PerInstanceInferenceReducer.class);
			
			// set both the map and reduce in/out classes
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			// but redefine them for the mapper		
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(DoubleArrayWritable.class);					
							
			s_logger.info("Job working directory is " + job.getWorkingDirectory());			
						
			return job.waitForCompletion(true) ? 0 : 1;
			
		}
		catch (IOException e)
		{					
			System.err.println("Something went badly wrong in IO.");
			System.exit(2);
		} catch (InterruptedException e) {
			System.err.println("Job interrupted.");
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			System.err.println("ClassNotFound exception.");
			e.printStackTrace();
		}
		
		return 2;
	}
	
	private String basename(String fName) {		
		return fName.substring(fName.lastIndexOf('/')+1); 
	}

	@Override
	public int run(String[] filteredargs) throws Exception {
				
		if (filteredargs.length != 3)
		{
			System.err.println("Usage: ExperimentDriver <network.xdsl> <dataset.csv> <output-dir>");
			
			String yousaid = "";
			for (String s : filteredargs)
			{				
				yousaid += s + " ";
			}
			System.err.println("You said to the driver: " + yousaid);
			System.err.println("Are those generic arguments supposed to be there?");
			System.exit(1);
		}
		
		networkFile_ = filteredargs[filteredargs.length-3];
		inPath_ = filteredargs[filteredargs.length-2];
		outPath_ = filteredargs[filteredargs.length-1];
						
		conf_ = getConf();
		conf_.set("keep.failed.task.files","true");			
		conf_.set("keep.failed.task.pattern","*");

				
		// ===================== Pass 1
		int presult = prepare();
		if (presult != 0)
		{
			s_logger.error("Prepare step failed");
			System.exit(4);
		}
		s_logger.info("Preparations suceeded.");
		
		// retrieve results of prepare		
		/* for (String a : attValueCounts_.keySet())
		{
			s_logger.info(a + "= ");
			for (String v : attValueCounts_.get(a).keySet())
			{
				s_logger.info("\t" + v + ": " + attValueCounts_.get(a).get(v));
			}				
		}*/
		
		// ===================== Pass 2		
		int iresult = inference();
		if (iresult != 0)
		{
			s_logger.error("Inference step failed");
			System.exit(8);
		}
						
		// retrieve results of inference from outPath_
				
		return iresult;		  	
	}
	
	public static void main(String args[]) throws Exception
	{	
		ToolRunner.run(new Configuration(), new ExperimentDriver(), args);
	}
}
