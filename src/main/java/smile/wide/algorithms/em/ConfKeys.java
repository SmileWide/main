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
package smile.wide.algorithms.em;

/**
 * Defines the configuration keys for distributed EM
 * @author shooltz@shooltz.com
 *
 */
public class ConfKeys {
	/**
	 * The input file in HDFS
	 */
	public static final String DATA_FILE = "em.data.file";
	
	/**
	 * The output directory in HDFS
	 */
	public static final String STAT_FILE = "em.stat.file";
	
	/**
	 * The separator used in the input file 
	 */
	public static final String SEPARATOR = "em.separator";
	
	/**
	 * The name of the local temporary file with merged reducer output.
	 */
	public static final String LOCAL_STAT_FILE = "em.local.stat.file";

	/**
	 * The XDSL file with initial network
	 */
	public static final String INITIAL_NET_FILE = "em.initial.netfile";
	
	/**
	 * The XDSL file modified by EM during the learning and containing the final output
	 */
	public static final String WORK_NET_FILE = "em.work.netfile";
	
	/**
	 * The columns names for the input data. If not present, the first line of the HDFS input
	 * is assumed to contain the column names (and will be skipped by mappers)
	 */
	public static final String COLUMNS = "em.columns";
	
	/**
	 * The token for missing value in the input data. Defaults to asterisk. Empty strings in the input data
	 * are also assumed to represent the missing value
	 */
	public static final String MISSING_TOKEN = "em.missing.token";
	
	/**
	 * Any value will mean that first line of the HDFS input file will be ignored. Will be set
	 * automatically if column names not explicitly provided.
	 */
	public static final String IGNORE_FIRST_LINE = "em.ignore.first.line";
}
