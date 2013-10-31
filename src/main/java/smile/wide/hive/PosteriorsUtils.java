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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;

import smile.Network;

/** An internal utility class encapsulating common functionality
 *  for both variants of posteriors UDFs
 * @author shooltz@shooltz.com
 */
class PosteriorsUtils {
	/** called at compile time by UDFs to validate the arguments
	 * 
	 * @param arguments  the argument type inspectors, known at compile time
	 * @return the ListObjectInspector if 2nd argument is an array, null otherwise
	 * @throws UDFArgumentException  if arguments do not follow the posteriors UDF syntax
	 */
	public static ListObjectInspector validateArguments(ObjectInspector[] arguments) throws UDFArgumentException {
		ListObjectInspector loi = null;
		if (arguments.length < 2) {
			throw new UDFArgumentLengthException("posteriors: at least 2 arguments needed");
		}
		
		for (int i = 0; i < arguments.length; i ++) {
			ObjectInspector oi = arguments[i];
			
			switch (i) {
			case 0:
				if (!(oi instanceof StringObjectInspector)) {
					throw new UDFArgumentTypeException(i, "posteriors: first argument must be a string");
				}
				break;
			case 1:
				boolean isList = oi instanceof ListObjectInspector;
				if (!(isList || oi instanceof StringObjectInspector || oi instanceof VoidObjectInspector)) {
					throw new UDFArgumentTypeException(i, "posteriors: second argument must be null, a string or an array of strings");	
				}
				
				if (isList) {
					loi = (ListObjectInspector)oi;
					if (!(loi.getListElementObjectInspector() instanceof StringObjectInspector)) {
						throw new UDFArgumentTypeException(i, "posteriors: elements of the second argument must be strings");
					}
				}
				break;
			default:
				if (!(oi instanceof PrimitiveObjectInspector)) {
					throw new UDFArgumentTypeException(i, "posteriors: arguments 3 and up must be of a primitive type");
				}
			}
		}
		
		return loi;
	}
	
	/** finds the node by idenfitier, case-insensitive
	 *  
	 * @param net  the network
	 * @param id  node identifier
	 * @return node handle or -1 if not found
	 */
	private static int findNode(Network net, String id) {
		for (int h = net.getFirstNode(); h >= 0; h = net.getNextNode(h)) {
			if (net.getNodeId(h).compareToIgnoreCase(id) == 0) {
				return h;
			}
		}
		return -1;
	}

	
	/** processes the single row of data passed as UDF arguments by loading the network,
	 * setting the targets and evidence and updating the beliefs. 
	 * 
	 * @param net  the network
	 * @param arguments   the UDF arguments: network filename, targets and node/outcome evidence pairs
	 * @param loi  a ListObjectInspector instance initialized at Hive compile time. If null, the targets argument is null or a string
	 * @return the list of target identifiers 
	 */
	public static List<?> processNetwork(Network net, Object[] arguments, ListObjectInspector loi) {
		List<?> targets = new ArrayList<String>();
		net.readFile(arguments[0].toString());

		boolean targetsSpecified = false;
		if (loi == null) {
			Object a1 = arguments[1];
			ArrayList<String> t = new ArrayList<String>();
			if (a1 == null) {
				for (String id: net.getAllNodeIds()) {
					if (net.isTarget(id)) {
						t.add(id);
					}
				}
				if (t.isEmpty()) {
					targets = Arrays.asList(net.getAllNodeIds());
				} else {
					targets = t;
				}
			} else {
				t.add(arguments[1].toString());
				targets = t;
				targetsSpecified = true;
			}
		} else {
			targets = loi.getList(arguments[1]);
			targetsSpecified = true;
		}
		
		if (targetsSpecified) {
			net.clearAllTargets();
			ArrayList<String> t2 = new ArrayList<String>(targets.size());
			for (Object o: targets) {
				int handle = findNode(net, o.toString());
				net.setTarget(handle, true);
				t2.add(net.getNodeId(handle)); 
			}
			targets = t2;
		}
		
		for (int i = 2; i < arguments.length; i += 2) {
			Object node = arguments[i];
			Object outcome = arguments[i + 1];
			net.setEvidence(findNode(net, node.toString()), outcome.toString());
		}
		
		net.updateBeliefs();
	
		return targets;
	}

}
