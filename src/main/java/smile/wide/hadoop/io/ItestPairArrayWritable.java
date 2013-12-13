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

import java.util.ArrayList;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import smile.wide.utils.Pair;

public class ItestPairArrayWritable extends ArrayWritable {
	
	public ItestPairArrayWritable() 
	{ 
		super(ItestPairWritable.class); 
	}

	public ItestPairArrayWritable(ItestPairWritable[] fw) {
		super(ItestPairWritable.class, fw);
	}

	public ItestPairArrayWritable(ArrayList<Pair<Pair<Integer, Integer>, ArrayList<Integer>>> t) {
		super(ItestPairWritable.class);
		ItestPairWritable[] array = new ItestPairWritable[t.size()];
		int cntr=0;
		for(Pair<Pair<Integer, Integer>, ArrayList<Integer>> q : t) {
			int x = q.getFirst().getFirst();
			int y = q.getFirst().getSecond();
			ArrayList<Integer> sepsets = q.getSecond();
			IntWritable[] a = new IntWritable[sepsets.size()];
			for(int i=0; i<a.length; i++)
			   a[i] = new IntWritable(sepsets.get(i));
			ItestPairWritable z = new ItestPairWritable(x,y,a);
			array[cntr++]=z;
		}
		set(array);
	}

	public int compareTo(ItestPairArrayWritable second) {
		Writable[] a = get();
		Writable[] b = second.get();
		if(a.length == b.length) {
			ItestPairWritable l,r;
			for(int x = 0 ;x < a.length;++x) {
				l = (ItestPairWritable) a[x];
				r = (ItestPairWritable) b[x];
				if(l.compareTo(r) != 0)
					return l.compareTo(r);
	 		}
			return 0;
		}
		else
			return a.length - b.length;
	}
	
	@Override
	public String toString() {
		String result = "";
		Writable[] a = get();
		ItestPairWritable l;
		result+="{";
		for(int x = 0 ;x < a.length;++x) {
			l = (ItestPairWritable) a[x];
			if(x > 0)
				result += "," + l.toString();
			else
				result += l.toString();
 		}
		result+="}";
		return result;
	}
}
