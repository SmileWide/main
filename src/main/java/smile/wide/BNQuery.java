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

import java.util.ArrayList;

/**
 * <p>
 * A representation of a Bayesian network query. Written as
 * <p>
 * 
 * <pre>
 * p(q|e<sub>1</sub>, e<sub>2</sub>, &hellip; e<sub>n</sub>)
 * </pre>
 * <p>
 * where q is the query variable and e<sub>1</sub>, e<sub>2</sub>, &hellip; e<sub>n</sub> are the
 * evidence variables. We currently support only one query variable, because the underlying SMILE
 * engine cannot do multivariate joint posteriors very well yet.
 * 
 * @author tomas.singliar@boeing.com
 * 
 */
public class BNQuery
{

private String queryVar_ = new String();

/** the names of variables **/
private ArrayList<String> evidenceVars_ = new ArrayList<String>();

/** forced evidence values **/
private ArrayList<String> evidenceValues_ = new ArrayList<String>();


// TODO: add the network in as well and validate inputs against the network

// ================================================================

public String getQueryVar()
	{
	return queryVar_;
	}


public void setQueryVar(String queryVar)
	{
	this.queryVar_ = queryVar;
	}


/**
 * Add a variable to the evidence part of the query.
 * The values for it will be retrieved from the dataset.
 * 
 * @param var	The variable's name.
 */
public void addEvidenceVariable(String var)
	{
	evidenceVars_.add(var);
	evidenceValues_.add(null);
	}


/**
 * Add a variable to the evidence part of the query, together with a forced value.
 * The values for it will NOT be retrieved from the dataset, but set to the given value.
 * 
 * @param var	The variable's name.
 * @param val	The variable's value.
 */
public void addEvidenceValue(String var, String val)
	{
	evidenceVars_.add(var);
	evidenceValues_.add(val);
	}


/**
 * Retrieve evidence variables.
 * 
 * @return the list of evidence variable
 */
public ArrayList<String> getEvidenceVars()
	{
	return evidenceVars_;
	}


/**
 * Retrieve evidence values
 * 
 * @return the list of evidence values - null where not set.
 */
public ArrayList<String> getEvidenceValues()
	{
	return evidenceValues_;
	}

}
