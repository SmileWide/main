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
package smile.wide.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RunCounter implements Tool {
	@Override
	public int run(String[] params) throws Exception {
		System.out.println("Tool: libpath=" + java.lang.System.getProperty("java.library.path"));
		for (int i = 0; i < params.length; i ++) {
			System.out.println("parameter " + i + ":" + params[i]);
		}
		
		Configuration conf = getConf();
		
		conf.set(MapCounter.MAPPER_CONFKEY_DATA_ORDER, params[2]);
		StringBuilder families = new StringBuilder();
		for (int i = 3; i < params.length; i ++) {
			if (families.length() > 0) {
				families.append('\t');
			}
			families.append(params[i]);
		}

		conf.set(MapCounter.MAPPER_CONFKEY_DATA_ORDER, params[2]);
		conf.set(MapCounter.MAPPER_CONFKEY_FAMILIES, families.toString());
		
		// job must be created AFTER Configuration.set calls
		Job job = new Job(conf);

		FileInputFormat.addInputPath(job, new Path(params[0]));	
		Path outputPath = new Path(params[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.setJarByClass(RunCounter.class);
		job.setMapperClass(MapCounter.class);
		job.setReducerClass(ReduceCounter.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.waitForCompletion(true);
		return 0;
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

	 public static void main(String[] args) throws Exception {
		 int exitCode = ToolRunner.run(new RunCounter(), args);
		 System.exit(exitCode);
	 }
}
