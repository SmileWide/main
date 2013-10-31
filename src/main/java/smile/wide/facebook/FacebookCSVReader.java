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
package smile.wide.facebook;

import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import smile.wide.data.DataSetReader;
import smile.wide.data.FilteringReader;
import smile.wide.data.Instance;


/** This is thee reader for the facebook dataset. 
 * Maps the lines of the facebook dataset into BN outcomes.  
 * 
 * @author yq498c
 *
 */
public class FacebookCSVReader 
	extends FilteringReader
	implements DataSetReader<Long, String>
{
	private static final Logger s_logger;
	
	static
	{
		s_logger = Logger.getLogger(FacebookCSVReader.class);
		s_logger.setLevel(Level.INFO);			
	}
	
	
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
	public Instance<Long, String> parseLine(String line) {
		
		// TODO: CSVReader would do the splitting more robustly,
		// but isn't needed for this dataset - no weird quotes
		String[] tokens = line.split(",");
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
		
		// do the Facebook dataset - specific discretization 
		String[] val = new String[values.size()];
		
		val[0] = cleanInput(values.get(0));		// 3 - firstName, string e
		val[1] = cleanInput(values.get(1));		// 5 - middlename, string
		val[2] = cleanInput(values.get(2));		// 7 - sex, string
		val[3] = cleanInput(values.get(3));		// 10 - isAppUser, boolean
				
		val[4] = interpret(cleanInput(values.get(4)), "LikesCount");	// 11 - likescount
		val[5] = interpret(cleanInput(values.get(5)), "FriendsCount");	// 12 - friendscount		
												
		return new Instance<Long,String>(ID, val);
	}

	private String cleanInput(String string) {
		return string.replace(".","").trim().toLowerCase();
	}

	@Override
	public String interpret(String value, String forAttribute) {
		
		if (value==null)
		{
			return null;
		}
		
		if ("FirstName".equals(forAttribute))
		{
			return value;
		}
		else if ("MiddleName".equals(forAttribute))
		{
			return value;
		}
		else if ("Sex".equals(forAttribute))
		{
			return value;
		}
		else if ("IsAppUser".equals(forAttribute))
		{
			return value;
		}
		else if ("LikesCount".equals(forAttribute))
		{
			int likesCount = Integer.parseInt(value);		
			return discretize(new int[] {-1, 100, 200, 500, 1000, 5000},		
													// boundaries, first value is for missing
								new String[] {"lt100", "c101_200", "c201_500", "c501_1000", "c1001_5000", "gt5000"},	// what to call the ranges
								likesCount);
		}
		else if ("FriendsCount".equals(forAttribute))
		{
			// discretize friendCount
			int friendCount = Integer.parseInt(value);		
			return discretize(new int[] {-1, 10, 20, 50, 100, 200, 500},		// boundaries, first value is for missing
											new String[] {"lt10", "c11_20", "c21_50", "c51_100", "c101_200", "c201_500", "gt500"},	// what to call the ranges
											friendCount);	 
		}
		else
		{
			s_logger.error("Cannot interpret '" + value + "' as unknown column " + forAttribute); 
			return null;
		}
		
		
	}
	
	
	// ===================================================================================
	// Main and tests
	
	/**
	 * A quick test with the facebook data
	 */
	public static void main(String[] args)
	{
		FacebookCSVReader fr = new FacebookCSVReader();
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

	@Override
	public boolean isMissingValue(String value, String forAtribute) 
	{		
		return (value == null || value.isEmpty());
	}
	
	
	
}
