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


public interface DataSetReader<K,V> {

	// ==========================================
	// this part about configuring the reader

	/**
	 * 
	 * @param filter - a string like 1,2,4-5, all - weka-style
	 * 
	 * 1-based index of the column in the un-filtered dataset
	 * 
	 */
	void setFilter(String filter);
	
	/**
	 * Which column represents the unique instance ID.
	 * This is required, because the map may require
	 * the instance ID for output to reduce.
	 * 
	 * 1-based index of the column in the un-filtered dataset
	 * So if filter is 1,3,5 and instanceID = 2, the second 
	 * column is used, and not part of the data presented to the
	 * Bayes network.
	 */
	void setInstanceIDColumn(int instanceIDcolumn);
	
	// ==========================================
	// this part about reading from a text file
	
	/** 
	 *	open the file to read from.
	 *  close previous file if open
	 *  
	 *  @return file successfully opened
	 */
	boolean setFile(String fileName);
	
	/** 
	 *	set file pointer back to start
	 *  
	 *  @return file successfully rewound
	 */
	boolean rewind();
	
	/** 
	 * 
	 * @return the next record as an instance
	 *         or null if end of file
	 * 
	 */
	Instance<K, V> nextRecord();
	
	// ==========================================
	// this part about parsing lines one by one
		
	Instance<K, V> parseLine(String value);

	// ==========================================
	// this part about interpreting the values	
	
	/** 
	 * Interpret a raw string value as a BN outcome.
	 * Must guarantee that output is a valid SMILE BN outcome identifier, 
	 * but need not specifically check that it is valid for a particular network.
	 * 
	 * @param value
	 * @return A Bayesian network outcome valid for the attribute.
	 */
	public abstract String interpret(String value, String forAttribute);
	
	/** 
	 * Does the given string represents a missing value for the given attribute?
	 * 
	 * @param value
	 * @param forAtribute
	 * @return	True iff the given string represent a missing value for the given attribute.
	 */
	public abstract boolean isMissingValue(String value, String forAtribute);
	
}
