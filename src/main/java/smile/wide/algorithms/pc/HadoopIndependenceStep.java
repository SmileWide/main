/**
 * 
 */
package smile.wide.algorithms.pc;

import java.util.ArrayList;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import smile.wide.data.DataSet;
import smile.wide.utils.Pattern;

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

		Configuration conf = new Configuration();
		conf.setBoolean("disc", disc);
		conf.setInt("maxAdjacency", maxAdjacency);
		conf.setFloat("significance", (float) significance);
		conf.set("datainput","/user/mdejongh/input");
		conf.set("countoutput","/user/mdejongh/counts");
		conf.set("processedcounts","/user/mdejongh/output");
		String[] args = {};
				
		ToolRunner.run(conf, new HadoopIndependenceJob(), args);
		//retrieve results here
		//independence tests
		//separator sets
	}

}
