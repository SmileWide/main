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
	public void dispose() {
		data.dispose();
	}
	
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
	@Override
	public int getNumberOfVariables() {
		return data.getVariableCount();
	}
	@Override
	public int getNumberOfRecords() {
		return data.getRecordCount();
	}
	@Override
	public boolean isDiscrete(int column) {
		// TODO Auto-generated method stub
		return true;
	}
	@Override
	public String[] getStateNames(int column) {
		return data.getStateNames(column);
	}
	@Override
	public int getInt(int column, int record) {
		return data.getInt(column, record);
	}
	@Override
	public double getDouble(int column, int record) {
		return data.getFloat(column, record);
	}
	@Override
	public String getId(int column) {
		return data.getVariableId(column);
	}
	@Override
	public boolean isMissing(int column, int record) {
		// TODO Auto-generated method stub
		return (data.getInt(column, record) == smile.learning.DataSet.DefaultMissingInt) || (data.getFloat(column, record) == smile.learning.DataSet.DefaultMissingFloat);
	}
	
}
