/**
 * 
 */
package smile.wide.algorithms.pc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import smile.wide.data.DataSet;
import smile.wide.utils.Pattern;
import smile.wide.utils.Pattern.EdgeType;

/**
 * @author m.a.dejongh@gmail.com
 *
 */
public class HadoopIndependenceStep extends IndependenceStep {
	/**basic code framework to execute hadoop style stuff will be replaced with actual code*/
	@Override
	public void execute(DataSet ds, Pattern pat, boolean disc, int maxAdjacency, double significance, ArrayList<ArrayList<Set<Integer>>> sepsets) throws Exception {
		//DataSet will probably contain an URI
		//Pattern needs to be serialized to be distributed
		//disc is an easy parameter
		//maxAdjacency is an easy parameter
		//significance is an easy parameter
		//sepsets need to be collected from result
		if(!disc) {
			//throw something
		}
		
		for(int adjacency = 0; adjacency <= maxAdjacency;++adjacency) {
			Configuration conf = new Configuration();
			conf.setBoolean("disc", disc);
			conf.setInt("maxAdjacency", adjacency);
			conf.setFloat("significance", (float) significance);
			conf.set("datainput","/user/mdejongh/input");
			conf.set("countoutput","/user/mdejongh/counts");
			conf.set("processedcounts","/user/mdejongh/pvalues");
			conf.set("maxpvalues","/user/mdejongh/output");
			conf.set("edgelist","edgelist.txt");
			conf.set("pattern",pat.toString());
			String[] args = {};
			
			ToolRunner.run(conf, new HadoopIndependenceJob(), args);
			//retrieve results here
			try {
				File file = new File("edgelist.txt");
				FileReader fileReader = new FileReader(file);
				BufferedReader bufferedReader = new BufferedReader(fileReader);
				String line;
				while ((line = bufferedReader.readLine()) != null) {
					String[] contents = line.split("\t");
					String[] variables = contents[0].replace("v","").split(",");
					int x = Integer.decode(variables[0]);
					int y = Integer.decode(variables[1]);
					pat.setEdge(x, y, EdgeType.None);
					pat.setEdge(y, x, EdgeType.None);
					if(maxAdjacency > 0) {
						String[] sepset = contents[1].replace("v","").replace("{","").replace("}", "").split(",");
						for(int z=0;z<sepset.length;++z) {
							int s = Integer.decode(sepset[z]);
							sepsets.get(x).get(y).add(s);
							sepsets.get(y).get(x).add(s);
						}
					}
				}
				fileReader.close();
				file.delete();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
