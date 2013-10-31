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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import smile.wide.facebook.ExperimentDriver;

/**
 * This class is intended as a base class for DataSetReaders, providing an attribute filtering
 * mechanism and a "discretize" helper function.
 * 
 * @author tomas.singliar@boeing.com
 * 
 */
public abstract class FilteringReader
{

// ==================================================================================
private static final Logger s_logger;
static
	{
	s_logger = Logger.getLogger(ExperimentDriver.class);
	s_logger.setLevel(Level.FATAL);
	}

protected int[] indices_ = null; // MAX_VALUE has special meaning - take all columns from here on
protected int instanceIDcolumn_ = -1;


// ==================================================================================

/**
 * parse a filter string, such as 1,2,4-7,9,11-end, denoting which attributes
 * (1-based) are to be read
 * 
 * @param filter
 */
public void setFilter(String filter)
	{

	String[] indexranges = filter.split(",");
	// now everything is of the format
	// * a single number OR
	// * "number-number" OR
	// * "number - end"

	int lastNumber = -1;
	ArrayList<Integer> indices = new ArrayList<Integer>();
	for (String range : indexranges)
		{
		try
			{
			int z = Integer.parseInt(range.trim());
			lastNumber = z;
			indices.add(z);
			}
		catch (NumberFormatException e)
			{
			String[] endpoints = range.split("-");
			if (endpoints.length != 2)
				{
				s_logger.fatal("Bad range specifier: " + range);
				}

			int z = Integer.parseInt(endpoints[0].trim());
			// don't catch the potential exception, let it bubble up as error
			if (z <= lastNumber)
				{
				s_logger.fatal("Filter should have ascending numbers. [1,2,3] good, [2,1,3] bad:  " + range);
				}
			indices.add(z);

			if (endpoints[1].trim().equals("end"))
				{
				indices.add(Integer.MAX_VALUE);
				}
			}
		}

	indices_ = new int[indices.size()];
	for (int j = 0; j < indices.size(); ++j)
		{
		indices_[j] = indices.get(j);
		}
	}


public void setInstanceIDColumn(int instanceIDcolumn)
	{

	if (instanceIDcolumn <= 0)
		{
		s_logger.fatal("Instance ID column should be integer > 0 ");
		}

	instanceIDcolumn_ = instanceIDcolumn;
	}


/**
 * Compute which bin a value falls in when discretizing, and return the name of that interval.
 * 
 * @param bounds	The bounds. The first bound is the value used to denote the missing value.
 * @param attValues	The name of each interval in 'bounds'.
 * @param value		The value to look for.
 * @return 			The name of the interval; or an empty string if not found.
 */
protected String discretize(int[] bounds, String[] attValues, int value)
	{
	if (bounds.length < 2)
		{
		// this bad
		s_logger.error("Your ways are in error - too few bounds to discretize - bounds[0] encodes missing value.");
		return "";
		}

	if (bounds.length != attValues.length)
		{
		s_logger.error("Your ways are in error - list of boundaries and values should be same size.");
		return "";
		}

	if (value == bounds[0])
		{
		// this value is missing
		return "";
		}

	// find the smallest i>=1 such that value is less that bounds[i]
	// and return attValues[i]

	// presumably attValues[0] means "less or equal than bounds[1]"
	// attValues[1] means more than bounds[1] but less or equal to than bound[2]
	// attValues[n] means more than bounds[n]

	for (int i = 1; i < bounds.length - 1; ++i)
		{
		if (bounds[i] > value)
			{
			return attValues[i];
			}
		}

	return attValues[attValues.length - 1];

	}

}
