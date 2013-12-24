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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.math3.util.ArithmeticUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import smile.wide.hadoop.io.PairDoubleWritable;

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
	String ord = "";
	String countfile = "";
	Set<Integer> parents = new HashSet<Integer>();
	ArrayList<Integer> order = new ArrayList<Integer>();

	List<ArrayList<Integer>> parcounts = null;
	List<ArrayList<Integer>> counts = null;
	List<HashSet<String>> cardinalities = null;

	/** Initializes class parameters*/
	@Override
	protected void setup(Context context) {
		//get configuration
		Configuration conf = context.getConfiguration();

		//get variable to examine
		x=conf.getInt("VarX", -1);
		
		//get total number of variables
		nvar = conf.getInt("nvar",0);
		
		//get name of count file
		countfile = conf.get("countfile");
		
		//get current parent set
		par = conf.get("parents","");
		if(par != ""){
			String[] sZ = par.split(",");
			for(int i=0;i<sZ.length;++i)
				parents.add(Integer.decode(sZ[i]));
		}
		parsize = parents.size()+1;

		//init count data structures
		parcounts = new ArrayList<ArrayList<Integer>>();
		counts = new ArrayList<ArrayList<Integer>>();
		cardinalities = new ArrayList<HashSet<String>>();
		for(int i=0;i<nvar;++i) {
			cardinalities.add(new HashSet<String>());
			parcounts.add(new ArrayList<Integer>());
			counts.add(new ArrayList<Integer>());
		}
		//set order
		ord = conf.get("order","");
			String[] sZ = ord.split(",");
			for(int i=0;i<sZ.length;++i)
				order.add(Integer.decode(sZ[i]));
		
		//Read count file
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
		
		//set r constant for variable
		r = cardinalities.get(x).size();
	}
	/**Mapper
	 */
	@Override
	protected void map(LongWritable key, Void value, Context context) throws IOException, InterruptedException {
		//get current candidate
		int candidate = (int) key.get();
		
		//check if candidate isnt var or its parents
		if(candidate != x && !parents.contains(candidate) && order.indexOf(candidate) < order.indexOf(x)) {
			//calculate K2 score
			double logscore = 0;
			double faclogr = ArithmeticUtils.factorialLog(r-1);
			for(Integer i: parcounts.get(candidate))
				logscore +=  faclogr - ArithmeticUtils.factorialLog(i+r-1);
			for(Integer i: counts.get(candidate))
				logscore += ArithmeticUtils.factorialLog(i);
			context.write(new VIntWritable(0),new PairDoubleWritable(candidate,logscore));
		}
	}
}
