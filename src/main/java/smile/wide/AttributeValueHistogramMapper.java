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

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import smile.wide.data.DataSetReader;
import smile.wide.data.Instance;


public class AttributeValueHistogramMapper 
	extends Mapper<LongWritable, Text, Text, MapWritable >
{
	
	private static final Logger s_logger;

	static
	{
		s_logger = Logger.getLogger(PerInstanceInferenceMapper.class);
		s_logger.setLevel(Level.INFO);	
	}
	
	// ==============================================================
	// Members 
	
	// configuration and initialization
	
	private Configuration conf_ = null;
	
	private String fileReaderClass_ = null;		// what class to use to read the file 
	private String fileReaderFilter_ = null;	// a filter - which columns to use
	private String[] columnNames_ = null;

	private boolean initializing_ = true;
	
	// helpers
	private DataSetReader<Integer,String> reader_;
	
	// ==============================================================
	
	@SuppressWarnings("unchecked")
	@Override
	public void map(LongWritable offsetkey, Text value, Context context)
	{		
		if (initializing_)
		{
			conf_ = context.getConfiguration();
			fileReaderClass_ = conf_.get("xdata.bayesnets.datasetreader.class");
			fileReaderFilter_ = conf_.get("xdata.bayesnets.datasetreader.filter");
			columnNames_ = conf_.get("xdata.bayesnets.datasetreader.variablenames").split(",");
			assertEquals(columnNames_.length, fileReaderFilter_.split(",").length);
			
			try {
				Object r = Class.forName(fileReaderClass_).newInstance();			
				reader_  = (DataSetReader<Integer,String>) r;  
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
			reader_.setInstanceIDColumn(1);	// doesn't matter, won't use
			
			initializing_ = false;			
		}
		
		// we're initialized
		
		Instance<Integer,String> inst = reader_.parseLine(value.toString());				
		String[] vals = inst.getValue();
		
		try {
			for (int i=0; i < vals.length; ++i)
			{
				MapWritable mw = new MapWritable();
				mw.put(new Text(vals[i]), new IntWritable(1));
				context.write(new Text(columnNames_[i]), mw);
			}
		} catch (IOException e) {
			s_logger.error("I/O exception writing  the map output");
			e.printStackTrace();
		} catch (InterruptedException e) {
			s_logger.error("Interrupted writing the map output");
			e.printStackTrace();
		} catch (NullPointerException e)
		{
			s_logger.error("Null pointer, probably unexpected data");
			s_logger.error("Instance ID = " + inst.getID());
			for (int i=0; i < inst.getValue().length; ++i)
			{
				s_logger.error("Attribute_" + i + " = " + inst.getValue()[i]);
			};
		}
		
		
		
	}

}
