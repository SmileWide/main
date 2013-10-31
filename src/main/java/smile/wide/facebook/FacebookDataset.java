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

import java.net.URI;

import smile.wide.data.DataSet;


/** This represents what we know about the makeup of the Facebook dataset. 
 *  
 *  TODO: maybe this is generic enough that a similar class could implement
 *  access to datasets managed by Hive.
 * 
 */
public class FacebookDataset extends DataSet {

	static String[] COLUMNS = { "UID", 				// 1			// int  
								"UserName", 		// 2			// string
								"FirstName", 		// 3			// string
								"LastName",			// 4			// string
								"MiddleName",		// 5			// string
								"Name",				// 6			// string
								"Sex",				// 7			// string
								"locale",			// 8			// string
								"NameFormat",		// 9			// string
								"IsAppUser",		// 10			// boolean								
								"LikesCount",		// 11			// int
								"FriendCount",		// 12			// int
								"DateOfInfo" 		// 13			// date, format 2012-09-26
								};
	
	private URI location_;
	
	// ===============================================================================
	// Construction 
	public FacebookDataset(URI loc)
	{
		location_ = loc;
	}
	
	// ===============================================================================
	// Implementing DataSet
	
	@Override
	public URI location() {
		return location_;
	}

	@Override
	public String getName() {
		return "Facebook Users";
	}

	@Override
	public String[] columns() {
		return COLUMNS;				
	}

	@Override
	public int indexOfColumn(String s) {
		for (int i = 0; i < COLUMNS.length; ++i)
		{
			if (COLUMNS[i].equals(s))
				return i+1;
		}
		return -1;
	}

	@Override
	public int instanceIDColumnIndex() {		
		return 1;
	}
	
	// ===============================================================================

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
}
