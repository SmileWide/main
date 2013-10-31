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
package smile.wide;

/**
 * The interface that a smile.Network provides.
 * 
 * @see <a href="http://genie.sis.pitt.edu/wiki/Network_General_Methods" target="blank">SMILE online docs: Network_General_Methods</a>
 * @see <a href="http://genie.sis.pitt.edu/wiki/SMILE.NET:_Network" target="blank">SMILE online docs: SMILE.NET:_Network</a>
 * 
 * @author robert.e.cranfill@boeing.com
 */
public interface NetworkIF
{

/**
    <p>Adds an arc from the parent to the child. 
    <p>For this function to succeed, all of the following conditions must be true:
        <ul>
        <li>the parent and the child are valid node handles, 
        <li>the arc will not create a cycle, 
        <li>there is not already an arc from the parent to the child, 
        and 
        <li>the child can have the parent as a parent given their type 
        (For example, a DSL_MAU node cannot have a DSL_CPT as a parent). 
        </ul>
        Affected nodes will be invalidated if relevance reasoning is active.
**/
	void addArc(int parentHandle, int childHandle);
	
    /** Adds a node of the specified type and with the specified id to the network. 
     * If the node is correctly created, the returning value will be its handle. 
     * The available node types are enumerated in the <tt>smile.Network.NodeType</tt> inner class. 
     * 
     * @param nodeType One of the values of smile.Network.NodeType.
     * @param nodeId
     * **/
	int addNode(int nodeType, String nodeId);
	
	/** Adds a new outcome with the specified ID to the specified node. 
	 * The outcome will be added at the end of the outcome's list. **/
	void addOutcome(int nodeHandle, String outcomeId);

	/** Adds a new outcome with the specified ID to the specified node. 
	 * The outcome will be added at the end of the outcome's list. **/
	void addOutcome(String nodeId, String outcomeId);
	
	/** Clears all the evidence in the network. 
	 * Affected nodes will be invalidated if relevance reasoning is active. **/
	void clearAllEvidence();
	
	/** Removes the specified outcome from the specified node. **/
	void deleteOutcome(int nodeHandle, int outcomeIndex);
	/** Removes the specified outcome from the specified node. **/
	void deleteOutcome(String nodeId, String outcomeId);
	
	/** Returns the handle of the specified node. **/
	int getNode(String nodeId);

	/** Returns the count of nodes in the network. **/
	int getNodeCount();

	/** <p>Returns the definition of the specified node.
     <p>This function will always return a vector (or a one-dimensional array). 
     If the node has one or more parents the multiple dimensions of the definitions will be encoded into a vector. 
     You have to use the information on the number of parents and the number of their outcomes to decode the probabilities.
     <p>The order of probabilities is given by considering the state of the first parent of the node as the most 
     significant coordinate (thinking of the coordinates in terms of bits), then the second parent, and so on, and 
     finally considering the coordinate of the node itself as the least significant one.  **/
	double[] getNodeDefinition(String nodeId);
	
	/** Returns the value matrix of the specified node. Before this method can be called value has to be updated. 
	 Update the network to validate all the values. **/
	double[] getNodeValue(int nodeHandle);
	/** Returns the value matrix of the specified node. Before this method can be called value has to be updated. 
	 Update the network to validate all the values. **/
	double[] getNodeValue(String nodeID);
	
	/** Returns the number of outcomes of the specified node. **/
	int getOutcomeCount(String nodeId);
	
	/** Returns an array of the IDs of all the outcomes of the specified node. **/
	String[] getOutcomeIds(int nodeHandle);
	/** Returns an array of the IDs of all the outcomes of the specified node. **/
	String[] getOutcomeIds(String nodeId);
	
	/** Fills the network with the contents of the specified file. The file name must include the full path if needed. **/
	void readFile(String filepath);
	
	/** Sets the specified outcome to be an evidence for the specified node. 
	An outcome with the probability of 0 (zero) cannot be set as evidence. **/
	void setEvidence(int nodeHandle, int outcomeIndex);
	/** Sets the specified outcome to be an evidence for the specified node. 
	An outcome with the probability of 0 (zero) cannot be set as evidence. **/
	void setEvidence(int nodeHandle, String outcomeId);
	
    /** <p>Sets the definition of the specified node. Remember to always pass an array of the correct size.
    <p>Normally, when a node has no parents, its definition is a vector (or a one-dimensional array). However, 
    the definition can be a multidimensional array. It happens when one or more nodes index the node's definition, 
    i.e., they are its parents. In that case those multiple dimensions have to be encoded into a vector.
    <p>The order of probabilities, is given by considering the state of the first parent of the node as the 
    most significant coordinate (thinking of the coordinates in terms of bits), then the second parent, and so on, 
    and finally considering the coordinate of the node itself as the least significant one.
    <p>Consult Tutorial 1 for an example on setting a node's definition. **/
	void setNodeDefinition(int nodeHandle, double[] definition);
	/** <p>Sets the definition of the specified node. Remember to always pass an array of the correct size.
	<p>Normally, when a node has no parents, its definition is a vector (or a one-dimensional array). However, 
	the definition can be a multidimensional array. It happens when one or more nodes index the node's definition, 
	i.e., they are its parents. In that case those multiple dimensions have to be encoded into a vector.
	<p>The order of probabilities, is given by considering the state of the first parent of the node as the 
	most significant coordinate (thinking of the coordinates in terms of bits), then the second parent, and so on, 
	and finally considering the coordinate of the node itself as the least significant one.
	<p>Consult Tutorial 1 for an example on setting a node's definition. **/
	void setNodeDefinition(String nodeId, double[] definition);

	/** Sets the ID of the specified outcome of the specified node. **/
	void setOutcomeId(int nodeHandle, int outcomeIndex, String id);
	
	/** Updates the network using an algorithm set by setBayesianNetworkAlgorithm() and setInfluenceDiagramAlgorithm() 
	 * methods. Only those nodes that are relevant to the nodes that are marked as targets will be updated. 
	 * If no nodes are marked as targets, all the nodes of the network will be updated. **/
	void updateBeliefs();
	
	/** Writes the contents of the network to the specified file. 
	 * The file name may include the full path if needed. **/
	void writeFile(String fileName);
}
