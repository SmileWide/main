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
package smile.wide.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import smile.Network;


/** Table-generating posteriors function
 * @author shooltz@shooltz.com
 */
@Description(name = "posteriorsT",
	value = "_FUNC_(network,targets[,node1,outcome1[,node2,outcome2..]]) returns posteriors for specified targets given the evidence",
	extended = "Use this UDF to get posteriors\n")
public class PosteriorsUDTF extends GenericUDTF {
	/* (non-Javadoc)
	 * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDTF#initialize(org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector[])
	 */
	@Override
	public StructObjectInspector  initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		loi = PosteriorsUtils.validateArguments(arguments);
		
		List<String> fieldNames = new ArrayList<String>();
		fieldNames.add("node");
		fieldNames.add("outcome");
		fieldNames.add("posterior");
		
		List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
		fieldInspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING));
		fieldInspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING));
		fieldInspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.DOUBLE));
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldInspectors);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDTF#process(java.lang.Object[])
	 */
	@Override
	public void process(Object[] arguments) throws HiveException {
		Network net = new Network();

		List<?> targets = PosteriorsUtils.processNetwork(net, arguments, loi);
		
		for (Object t: targets) {
			String nodeId = t.toString();
			double[] p = net.getNodeValue(nodeId);
			String[] outcomes = net.getOutcomeIds(nodeId);

			for (int i = 0; i < outcomes.length; i ++) {
				forward(new Object[] { nodeId, outcomes[i], p[i] });
			}
		}
		
		net.dispose();
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDTF#close()
	 */
	@Override
	public void close() throws HiveException {
	}
	
	/** object inspector for the 2nd function argument representing the target set,
	 * which may be null, a string or an array of strings
	 */
	private ListObjectInspector loi;
}
