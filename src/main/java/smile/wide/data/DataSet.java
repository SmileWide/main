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
package smile.wide.data;

import java.net.URI;

/** The interface base class for various datasets.
 *  Generalizes datasets residing in files, Hive, streaming ....
 *  
 *  This is left as a skeleton because XDATA will surely will deal with this in some common way.
 *   
 * @author tomas.singliar@boeing.com
 */
public abstract class DataSet {
	
	// TODO: add some sort of general interface to access specific data elements (however implemented?)
	
	// TODO: the dataset should be able to describe itself: 
	//       - are you a matrix with named columns?
	//		 - what are the names and types of your columns?
	//		 - for each column, which value represents missing data?
	//		 - is there an instance ID column, and which is it?
	
	// should Dataset and DataSetReader be merged?
	
	// subclasses should probably know how to parse the underlying serialized form	
	
	/** 
	 * Where is this dataset accessible?
	 * 
	 * @return A URI for the location of this dataset.
	 */
	abstract public URI location();
	
	/** Does this dataset have a neame?
	 *  
	 * @return the name of the dataset, or "" if none
	 */
	abstract public String getName();
	
	/** List column names, in the order of increasing column index
	 * 
	 * @return the list
	 */
	abstract public String[] columns();
	
	/** At which index does the named column live. Index is 1-based.
	 * 
	 * @return the column index, -1 if none
	 */
	abstract public int indexOfColumn(String s);
	
	/** Which column is to serve to uniquely identify instances. Index is 1-based.
	 * 
	 * @return the column index, -1 if none
	 */
	abstract public int instanceIDColumnIndex();
	
}
