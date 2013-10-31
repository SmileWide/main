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

import java.util.ArrayList;

import smile.wide.data.DataSetReader;
import smile.wide.data.FilteringReader;
import smile.wide.data.Instance;


/** A reader class that does not do any interpretation of the input.
 * 
 * @author yq498c
 *
 */
public class CSVtoStringReader 
	extends FilteringReader
	implements DataSetReader<Long, String>
{
	@Override
	public boolean setFile(String fileName) {
		return false;			// NOT Implemented
	}

	@Override
	public boolean rewind() {
		return false; 			// NOT Implemented
	}

	@Override
	public Instance<Long, String> nextRecord() {
		return null;			// NOT Implemented
	}

	
	@Override
	public Instance<Long, String> parseLine(String value) {
		
		// TODO: CSVReader would do the splitting more robustly		
		String[] tokens = value.split(",");
		long ID = Long.parseLong(tokens[instanceIDcolumn_ - 1]);
		
		ArrayList<String> values = new ArrayList<String>();
		for (int k=0; k < indices_.length; ++k)
		{
			if (indices_[k] < Integer.MAX_VALUE)
			{
				values.add(tokens[indices_[k] - 1]);
			}
			else
			{
				// if you see a MAX_VALUE, add all columns from the last
				// number seen to the end of the row
				for (int m=indices_[k-1]; m < tokens.length; ++m)
				{
					values.add(tokens[m-1]);
				}
				// and you are done
				break;
			}
		}
		  
		String[] val = new String[values.size()];
		for (int k=0; k < values.size(); ++k)
		{
			val[k] = values.get(k);
		}
						
		return new Instance<Long,String>(ID, val);
		
	}
	
	/** Interpret a raw string value as a BN outcome, ignoring any processing.
	 *  Useful for reading files saved from GENIE, which are already cleanly written. 
	 * 
	 * @param value
	 * @return just return the value back 
	 */
	public String interpret(String value, String forAttribute)
	{
		return value;
	}

	@Override
	public boolean isMissingValue(String value, String forAtribute) {
		return (value == null || value.equals(""));
	}
	
	// ==========================================================================================
	
	/**
	 * A quick test with the facebook data
	 */
	public static void main(String[] args)
	{
		CSVtoStringReader fr = new CSVtoStringReader();
		fr.setFilter("3,5,7,10,11,12");
		fr.setInstanceIDColumn(1);
		
		Instance<Long,String> i1 = fr.parseLine("4,zuck,Mark,Zuckerberg,,Mark Zuckerberg,male,en_US,{first} {last},False,-1,-1,2012-09-26");
		System.out.println(i1.getID());
		for (String s : i1.getValue())
		{
			System.out.println(s);
		}
		
		// Instance<Long,String> i2 = fr.parseLine("5,ChrisHughes,Chris,Hughes,,Chris Hughes,male,en_US,{first} {last},False,-1,1999,2012-09-26");
	}	
	
}
