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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import smile.Network;


/** Scalar posteriors function
 * @author shooltz@shooltz.com
 */
@Description(name = "posteriors",
	value = "_FUNC_(network,targets[,node1,outcome1[,node2,outcome2..]]) returns posteriors for specified targets given the evidence",
	extended = "Use this UDF to get posteriors\n")
public class PosteriorsUDF extends GenericUDF {
	/* (non-Javadoc)
	 * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDF#initialize(org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector[])
	 */
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		loi = PosteriorsUtils.validateArguments(arguments);
		
		List<String> fieldNames = new ArrayList<String>();
		fieldNames.add("node");
		fieldNames.add("posteriors");
		
		List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
		fieldInspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING));
		fieldInspectors.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.DOUBLE)));
		return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldInspectors));
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDF#evaluate(org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject[])
	 */
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object[] actualArguments = new Object[arguments.length];
		for (int i = 0; i < arguments.length; i ++) {
			actualArguments[i] = arguments[i].get();
		}

		Network net = new Network();
		List<?> targets = PosteriorsUtils.processNetwork(net, actualArguments, loi);
		
		ArrayList<Object[]> out = new ArrayList<Object[]>();
		
		for (Object t: targets) {
			String nodeId = t.toString();
			double[] p = net.getNodeValue(nodeId);
			Object[] p2 = new Object[p.length];
			for (int i = 0; i < p.length; i ++) {
				p2[i] = new Double(p[i]);
			}
			
			Object[] item = new Object[2];
			item[0] = nodeId;
			item[1] = p2;

			out.add(item);
		}
		
		net.dispose();
		
		return out;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDF#getDisplayString(java.lang.String[])
	 */
	@Override
	public String getDisplayString(String[] children) {
		StringBuilder sb = new StringBuilder();
		sb.append("posteriors(");
		for (int i = 0; i < children.length; i++) {
			if (i > 0) {
				sb.append(',');
			}
			sb.append(children[i]);
		}
		return sb.toString();
	}

	/** object inspector for the 2nd function argument representing the target set,
	 * which may be null, a string or an array of strings
	 */
	private ListObjectInspector loi; 
}
