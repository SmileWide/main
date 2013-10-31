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
package smile.wide.hadoop.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Progressable;


public class LastRecordOnlyOutputFormat<K,V>
extends TextOutputFormat<K,V> 
{

	// only writes out the last record, when close() is called.
	protected static class LastRecordWriter<K, V>
	implements RecordWriter<K, V> 
	{
		private static final String utf8 = "UTF-8";
		private static final byte[] newline;
		static {
			try {
				newline = "\n".getBytes(utf8);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8 + " encoding");
			}
		}

		private K _key;
		private V _value;

		protected DataOutputStream out;
		private final byte[] keyValueSeparator;

		public LastRecordWriter(DataOutputStream out, String keyValueSeparator) {
			this.out = out;
			try {
				this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8 + " encoding");
			}
		}

		public LastRecordWriter(DataOutputStream out) {
			this(out, "\t");
		}

		/**
		 * Write the object to the byte stream, handling Text as a special
		 * case.
		 * @param o the object to print
		 * @throws IOException if the write throws, we pass it on
		 */
		private void writeObject(Object o) throws IOException {
			if (o instanceof Text) {
				Text to = (Text) o;
				out.write(to.getBytes(), 0, to.getLength());
			} else {
				out.write(o.toString().getBytes(utf8));
			}
		}

		public synchronized void write(K key, V value)
		{
			_key = key;
			_value = value;
		}

		private void writeForReal()
				throws IOException {

			boolean nullKey = _key == null || _key instanceof NullWritable;
			boolean nullValue = _value == null || _value instanceof NullWritable;
			if (nullKey && nullValue) {
				return;
			}
			if (!nullKey) {
				writeObject(_key);
			}
			if (!(nullKey || nullValue)) {
				out.write(keyValueSeparator);
			}
			if (!nullValue) {
				writeObject(_value);
			}
			out.write(newline);
		}

		public synchronized void close(Reporter reporter) throws IOException {
			writeForReal();
			out.close();
		}
	}

	public RecordWriter<K, V> getRecordWriter(FileSystem ignored,
			JobConf job,
			String name,
			Progressable progress)
	throws IOException 
	{		
		String keyValueSeparator = job.get("mapreduce.output.textoutputformat.separator", 
				"\t");

		Path file = FileOutputFormat.getTaskOutputPath(job, name);
		FileSystem fs = file.getFileSystem(job);
		FSDataOutputStream fileOut = fs.create(file, progress);
		return new LastRecordWriter<K, V>(fileOut, keyValueSeparator);

	}
}
