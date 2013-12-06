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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.util.ArithmeticUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import smile.wide.utils.PairDoubleWritable;

/**
 * Mapper
 * @author m.a.dejongh@gmail.com
 *
 */
public class FangStructureScoreMapper extends Mapper<LongWritable, Void, VIntWritable, PairDoubleWritable> {
	int x = 0;
	int nvar = 0;
	int parsize = 0;
	int r = 0;
	String par = "";
	String countfile = "";
	Set<Integer> parents = new HashSet<Integer>();

	List<ArrayList<Integer>> parcounts = null;
	List<ArrayList<Integer>> counts = null;
	List<HashSet<String>> cardinalities = null;

	/** Initializes class parameters*/
	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		//set some constants here
		x=conf.getInt("VarX", -1);
		nvar = conf.getInt("nvar",0);
		countfile = conf.get("countfile");
		par = conf.get("parents","");
		if(par != ""){
			String[] sZ = par.split(",");
			for(int i=0;i<sZ.length;++i)
				parents.add(Integer.decode(sZ[i]));
		}
		parsize = parents.size()+1;
		//here read file and put contents in a useful datastructure
		//retrieve results here
		parcounts = new ArrayList<ArrayList<Integer>>();
		counts = new ArrayList<ArrayList<Integer>>();
		cardinalities = new ArrayList<HashSet<String>>();

		for(int i=0;i<nvar;++i) {
			cardinalities.add(new HashSet<String>());
			parcounts.add(new ArrayList<Integer>());
			counts.add(new ArrayList<Integer>());
		}
		try {
			File file = new File(countfile);
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				String[] contents = line.split("\t");
				String[] assignments = contents[0].split("\\+");
				int countlength = assignments.length;
				String[] parts = assignments[0].split("=");
				int index = Integer.decode(parts[0].substring(1));
				if(countlength == parsize)
					parcounts.get(index).add(Integer.decode(contents[1]));
				else
					counts.get(index).add(Integer.decode(contents[1]));
				for(int i=0;i<assignments.length;++i) {
					parts = assignments[i].split("=");
					index = Integer.decode(parts[0].substring(1));
					cardinalities.get(index).add(parts[1]);
				}
			}
			fileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		r = cardinalities.get(x).size();
	}
	/**Mapper
	 */
	@Override
	protected void map(LongWritable key, Void value, Context context) throws IOException, InterruptedException {
		int candidate = (int) key.get();
		if(candidate != x && !parents.contains(candidate)) {
			double logscore = 0;
			//format is y+parents+x
			/*
			 * We need to calculate K2 here.
			 * g(PI) = PROD^{q}_{j=1}{(r-1)!}/{(Nj+r-1)!} PROD^{r}_{k=1}Njk!
			 */
			double faclogr = ArithmeticUtils.factorialLog(r-1);
			for(Integer i: parcounts.get(candidate))
				logscore +=  faclogr - ArithmeticUtils.factorialLog(i+r-1);
			for(Integer i: counts.get(candidate))
				logscore += ArithmeticUtils.factorialLog(i);
			context.write(new VIntWritable(0),new PairDoubleWritable(candidate,logscore));
		}
	}
}
