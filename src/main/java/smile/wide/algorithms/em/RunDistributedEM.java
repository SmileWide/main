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
package smile.wide.algorithms.em;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import smile.Network;
import smile.wide.hadoop.io.DoubleArrayWritable;


/**
 * The runner class for the distributed parameter learning.
 * Schedules Hadoop jobs until the probabilities converge.
 * Execute with hadoop jar command. The EM-specific command line
 * parameters are passed as key=value arguments with key prefixed by 'em.'
 * Supported options are defined in the {@link ConfKeys} class.
 * 
 * @author shooltz@shooltz.com
 *
 */
public class RunDistributedEM implements Tool {
	public RunDistributedEM(int iteration) {
		this.iteration = iteration;
	}
	
	private int iteration;
	private double score;
	
	private double getScore() {
		return score;
	}


	/**
	 * Scans for key=value pairs in the command line and processes these prefixed with 'em.'
	 * @param params command line parameters
	 */
	private void initParams(String[] params) {
		for (String p: params) {
			String[] keyValue = p.split("=");
			getConf().set(keyValue[0], keyValue[1]);
		}
	}
	
	/**
	 * Creates a copy of the input file with uniformized parameters
	 * @param inputName name of the input network file
	 * @param outputName name of the output network file
	 */
	private static void makeUniformCopy(String inputName, String outputName) {
		Network net = new Network();
		net.readFile(inputName);

		for (int h = net.getFirstNode(); h >= 0; h = net.getNextNode(h)) {
			double[] def = net.getNodeDefinition(h);
			int outcomeCount = net.getOutcomeCount(h);
			Arrays.fill(def, 1.0 / outcomeCount);
			net.setNodeDefinition(h, def);
		}
		
		net.writeFile(outputName);
	}
	
	/**
	 * Reads the first line of the file, used to get the data column names
	 * @param fs the input filesystem
	 * @param filename the name of the input file 
	 * @return
	 * @throws IOException
	 */
	private static String readFirstLine(FileSystem fs, String filename) throws IOException {
		String line;
		FSDataInputStream in = fs.open(new Path(filename));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		line = br.readLine();
		return line;
	}
	
	/**
	 * Helper method for uniformized configuration param access
	 * @param key
	 * @return
	 */
	private String getParam(String key) {
		return getConf().get(key);
	}
	
	/**
	 * Copies the values from the local probabilities file into XDSL network file
	 * @param netFilename temporary network file
	 * @param paramFilename temporary merged probabilities file
	 * @return log likelihood
	 * @throws IOException
	 */
	private static double updateParameters(String netFilename, String paramFilename) throws IOException {
		double logLik = 0;
		
		Network net = new Network();
		net.readFile(netFilename);
		BufferedReader br = new BufferedReader(new FileReader(paramFilename));
		String line;
		while ((line = br.readLine()) != null) {
			String[] hp = line.split("\t");
			int handle = Integer.parseInt(hp[0]);
			if (handle < 0) {
				logLik = Double.parseDouble(hp[1]);
			} else {
				String[] params = hp[1].split(" ");
				int count = params.length;
				double[] def = new double[count];
				for (int i = 0; i < count; i ++) {
					def[i] = Double.parseDouble(params[i]);
				}
				net.setNodeDefinition(handle, def);
			}
		}
		br.close();

		net.writeFile(netFilename);
		return logLik;
	}
	
	/**
	 * Initializes the job configuration and starts the job.
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] params) throws Exception {
		for (int i = 0; i < params.length; i ++) {
			System.out.println("parameter " + i + ":" + params[i]);
		}

		initParams(params);
		
		if (getParam(ConfKeys.COLUMNS) == null) {
			conf.set(ConfKeys.COLUMNS, readFirstLine(FileSystem.get(conf), getParam(ConfKeys.DATA_FILE)));
			conf.set(ConfKeys.IGNORE_FIRST_LINE, "true");
		}
		
		if (getParam(ConfKeys.MISSING_TOKEN) == null) {
			conf.set(ConfKeys.MISSING_TOKEN, "*");
		}
		
		if (getParam(ConfKeys.SEPARATOR) == null) {
			conf.set(ConfKeys.SEPARATOR, " ");
		} else {
			String s = getParam(ConfKeys.SEPARATOR);
			if (Character.isDigit(s.charAt(0))) {
				int code = Integer.parseInt(s);
				conf.set(ConfKeys.SEPARATOR, new String(Character.toChars(code)));
			}
		}
		
		System.out.println("Configuration map:");
		Iterator<Entry<String, String>> iter = conf.iterator();
		while (iter.hasNext()) {
			Entry<String, String> e = iter.next();
			if (e.getKey().startsWith("em.")) {
				System.out.println(e.getKey() + "=\"" + e.getValue() + "\"");
			}
		}
		
		Job job = new Job(conf);
		job.setJobName("smile-wide-em, iter " + iteration + " on " + getParam(ConfKeys.INITIAL_NET_FILE));

		FileInputFormat.addInputPath(job, new Path(getParam(ConfKeys.DATA_FILE)));	
		Path outputPath = new Path(getParam(ConfKeys.STAT_FILE));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		job.setJarByClass(RunDistributedEM.class);
		job.setMapperClass(StatEstimator.class);
		job.setCombinerClass(StatCombiner.class);
		job.setReducerClass(StatNormalizer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleArrayWritable.class);
		job.waitForCompletion(true);

		FileSystem dfs = FileSystem.get(conf);
		System.out.println(dfs);
		LocalFileSystem lfs = LocalFileSystem.getLocal(conf);
		System.out.println(lfs);
		
		String localStatFile = getParam(ConfKeys.LOCAL_STAT_FILE);
		FileUtil.fullyDelete(new File(localStatFile));
		FileUtil.copyMerge(dfs, new Path(getParam(ConfKeys.STAT_FILE)), lfs, new Path(localStatFile), false, conf, null);

		score = updateParameters(getParam(ConfKeys.WORK_NET_FILE), localStatFile);
		System.out.println(">>>>>>  FINISHED ITERATION:" + iteration + ", logLik=" + score);
		return 0;
	}
	
	/**
	 * Checks for the convergence
	 * @param logLik
	 * @param prevLogLik
	 * @return
	 */
	private static boolean converged(double logLik, double prevLogLik)
	{
		if (logLik == 0)
		{
			return true;
		}
		else if (prevLogLik == 0)
		{
			return false;
		}
		else
		{
			double ratio = prevLogLik / logLik;
			return logLik <= prevLogLik || ratio < 1.0001;
		}
	}

	
	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
	
	private Configuration conf = new Configuration();

	/** 
	 * Runs the distributed EM by scheduling Hadoop jobs until convergence.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		System.out.println("Starting distributed EM, libpath=" + java.lang.System.getProperty("java.library.path"));

		String initFile = null;
		String workFile = null;
		for (String s: args) {
			if (s.startsWith(ConfKeys.INITIAL_NET_FILE)) {
				initFile = s.split("=")[1];
			} else if (s.startsWith(ConfKeys.WORK_NET_FILE)) {
				workFile = s.split("=")[1];
			}
		}

		System.out.println("Copying " + initFile + " to " + workFile);
		makeUniformCopy(initFile, workFile);
		
		double prevScore  = 1;
		double score = 0;
		int iter = 0;
		do {
			RunDistributedEM j = new RunDistributedEM(iter ++);
			ToolRunner.run(j, args);
			prevScore = score;
			score = j.getScore();
		} while (!converged(score, prevScore));
		System.out.println("Convergence in " + iter + " iterations.");
	}
}
