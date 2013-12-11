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

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntArrayWritable extends ArrayWritable {
	
	public IntArrayWritable() 
	{ 
		super(IntWritable.class); 
	}

	public IntArrayWritable(IntWritable[] fw) {
		super(IntWritable.class, fw);
	}

	public int compareTo(IntArrayWritable second) {
		Writable[] a = get();
		Writable[] b = second.get();
		if(a.length == b.length) {
			IntWritable l,r;
			for(int x = 0 ;x < a.length;++x) {
				l = (IntWritable) a[x];
				r = (IntWritable) b[x];
				if(l.compareTo(r) != 0)
					return l.compareTo(r);
	 		}
			return 0;
		}
		else
			return a.length - b.length;
	} 
}
