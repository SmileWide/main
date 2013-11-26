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
package smile.wide.algorithms.fang;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 * Mapper
 * @author m.a.dejongh@gmail.com
 *
 */
public class FangCounterMapper extends Mapper<LongWritable, Text, Text, VIntWritable> {
	String record = new String();
	int maxsetsize = 0;
	VIntWritable one = new VIntWritable(1);
	/** Initializes class parameters*/
	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		//set some constants here
		maxsetsize = conf.getInt("maxsetsize", 0);
	}
	
	/**Mapper
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
		record = value.toString();
		String[] values = record.split(",|\t| ");
		setFunction(context,maxsetsize,values);
	}
	
	void setFunction(Context context, int card, String[] vals) throws IOException, InterruptedException {
		int nvar = vals.length;
        // populate elements vector
        ArrayList<Integer> elements=new ArrayList<Integer>();
        int i;
        for (i = 0; i < nvar; i++)
        	elements.add(i);

        // check for enough elements
        if ((int) elements.size() < card) {
            return;
        }
        // generate conditioning sets
        ArrayList<Boolean> binvec = new ArrayList<Boolean>(Collections.nCopies(elements.size(), false));
        for (i = 0; i < card; i++) {
            binvec.set(i,true);
        }
        // test for conditional independence
        boolean first = true;
    	MutableInt cur = new MutableInt(card - 1);
    	MutableInt ones = new MutableInt(0);
        for (; first || NxtSubset(binvec, cur, ones); ) {
            first = false;
			String sepset = "";
            for (i = 0; i < (int) binvec.size(); i++) {
                if (binvec.get(i)) {
                    int z = elements.get(i);
                    if(sepset!="")
                    	sepset += "+v"+z+"="+vals[z];
                    else
                    	sepset += "v"+z+"="+vals[z];
                }
            }
            context.write(new Text(sepset), one);
        }
	}
	
    private boolean NxtSubset(ArrayList<Boolean> binvec, MutableInt cur, MutableInt ones) {
    	if (cur.intValue() < 0)	{
    		return false;
    	}
    	int size = binvec.size();
    	if ((cur.intValue() + 1 < size) && !binvec.get(cur.intValue() + 1))	{
    		binvec.set(cur.intValue(),false);
    		cur.increment();
    		binvec.set(cur.intValue(),true);
    		return true;
    	}
    	else {
    		ones.increment();
    		for (int i = cur.intValue() - 1; i >= 0; i--) {
    			if (binvec.get(i)) {
    				if (!binvec.get(i + 1))	{
    					binvec.set(i,false);
    					binvec.set(i + 1,true);
    					int j;
    					for (j = i + 2; j < (i + ones.intValue() + 2); j++)	{
    						binvec.set(j,true);
    					}
    					cur.setValue(j - 1);
    					ones.setValue(0);
    					for (; j < size; j++) {
    						binvec.set(j,false);
    					}
    					return true;
    				}
    				else {
    					ones.increment();
    				}
    			}
    		}
    		return false;
    	}
    }
}
