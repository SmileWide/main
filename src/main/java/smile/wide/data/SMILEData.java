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
import java.net.URISyntaxException;

/** Subclass of Dataset that wraps around the SMILE DataSet
 * 
 * @author m.a.dejongh@gmail.com
 * @author tomas.singliar@boeing.com
 *
 */
public class SMILEData extends DataSet {
	/** SMILE DataSet class*/
	private smile.learning.DataSet data = null;
	private  String fileName_ = null;				// most recent location
	
	// =========================================================================
	public void Read(String filename) {
		if(data == null)
			data = new smile.learning.DataSet();
		data.readFile(filename);
		fileName_ = filename;
	}
	/** Write data from SMILE's data object into a text file*/
	public void Write(String filename) {
		if(data != null)
			data.writeFile(filename);
		fileName_ = filename;
	}
	/** Takes a SMILE data object and replaces the internal data object with the this one*/
	public smile.learning.DataSet getData() {
		return data;
	}
	
	public void setData(smile.learning.DataSet data) {
		this.data = data;
	}
		
	// =========================================================================
	@Override
	public URI location() {		
		
		try {
			// TODO: convert to HDFS url if applicable
			return new URI("file://" + fileName_);
		} catch (URISyntaxException e) {			
			e.printStackTrace();
			return null;
		}
		
	}

	@Override
	public String getName() {		
		return "A SMILE dataset";
	}

	@Override
	public String[] columns() {
		int varcount =  data.getVariableCount();
		String[] cols = new String[varcount];
		for (int i = 0; i < varcount; ++i)
		{
			cols[i] = data.getVariableId(i);
		}
		return cols;
	}

	@Override
	public int indexOfColumn(String s) {
		String[] cols = columns();
		
		if (s == null)
			return -1;
		
		for (int i = 0; i < cols.length; ++i)
		{
			if (s.equals(cols[i]))
				return i;
		}
		
		return -1;
	}

	@Override
	public int instanceIDColumnIndex() {		
		return -1;
	}
	
}
