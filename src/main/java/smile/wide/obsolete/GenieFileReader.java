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

package smile.wide.obsolete;

import smile.Network;
import smile.wide.data.Instance;

import java.io.BufferedReader;
import java.io.FileReader;

import java.util.ArrayList;
import java.util.List;


@Deprecated
public class GenieFileReader
	{

	/**
	 * Reads in the file produced by Genie - pretty straightforward
	 * comma/tab-separated format. Expects the first column to be the unique ID
	 * and checks uniqueness. Can generate the unique ID if told so. Outputs for
	 * each row its unique ID and the attribute value indices (wants to access
	 * the network to translate strings to indices per the network definition
	 * 
	 * @param net- the Bayesian network
	 * @param file - the file from which to read data
	 * @param makeIDs
	 *            - whether to make unique instance IDs true - assumes the first
	 *            entry on a line is a data value, not an ID false - assumes the
	 *            first entry is an ID
	 * @param separator: ",", "\t", or other separator character
	 * @return
	 * 
	 */
	public List<Instance<Integer, Integer>> readFile(Network net, String fileName, boolean makeIDs, String separator)
		{
		int lastLength = -1;
		int lineNo = 1;
		int attributesStart = makeIDs ? 0 : 1;

		String[][] outcomeIds = new String[(net.getNodeCount())][];
		List<Instance<Integer, Integer>> dataset = new ArrayList<Instance<Integer, Integer>>();

		String line = null;
		BufferedReader fi = null;
		try
			{
			fi = new BufferedReader(new FileReader(fileName));
			String header = fi.readLine();
			String[] columnNames = header.split(separator);

			// map column names to Bayesian network node handles
			int[] nodeHandles = new int[columnNames.length];
			for (int c = attributesStart; c < columnNames.length; ++c)
				{
				nodeHandles[c] = net.getNode(columnNames[c]);
				}

			line = fi.readLine();
			while (line != null)
				{
				lineNo++;

				// parse the line string, check length
				String[] tokens = line.split(separator);
				if (lastLength != tokens.length && lastLength > 0)
					{
					System.err.printf("GenieFileReader.readFile: Line length differs at line %d\n", lineNo);
					}
				lastLength = tokens.length;

				// figure out the instance id

				int id;
				if (makeIDs)
					{
					attributesStart = 0;
					id = 7;
					fi.close();
					throw new Exception("Not implemented");
					} else
					{
					id = Integer.parseInt(tokens[0]);
					}

				Integer[] attributes = new Integer[lastLength - attributesStart];
				for (int i = attributesStart; i < tokens.length; ++i)
					{
					// i indexes tokens
					// attrIdx indexes attributes (nodes in the network)
					// j indexes outcome values

					int attrIdx = i - attributesStart;

					// pull out the outcome IDs if we don't have them
					if (outcomeIds[attrIdx] == null)
						{
						outcomeIds[attrIdx] = net.getOutcomeIds(nodeHandles[i]);
						}

					// search linearly for the outcome named like the token
					attributes[attrIdx] = Integer.MIN_VALUE; // initialize as
																// invalid
					for (int j = 0; j < outcomeIds[attrIdx].length; ++j)
						{
						if (tokens[i].equals(outcomeIds[attrIdx][j]))
							{
							attributes[attrIdx] = j;
							break;
							}

						if (tokens[i].equals("*"))
							{
							// * denotes missing value
							attributes[attrIdx] = null;
							break;
							}
						}
					if (attributes[attrIdx] != null && attributes[attrIdx].equals(Integer.MIN_VALUE))
						{
						System.err.printf("GenieFileReader.readFile: Unknown attribute value '%s' at line %d\n",
								tokens[i], lineNo);
						System.err.printf("Valid attribute values are:\n");
						for (int j = 0; j < outcomeIds[attrIdx].length; ++j)
							{
							System.err.printf("\t %s\n", outcomeIds[attrIdx][j]);
							}
						}
					}

				Instance<Integer, Integer> inst = new Instance<Integer, Integer>(id, attributes);
				dataset.add(inst);

				line = fi.readLine();
				} // while

			fi.close();

			} // try
		catch (Exception e)
			{
			System.err.printf("GenieFileReader.readFile: Miscellaneous error at line %d\n", lineNo);
			System.err.printf("Line was '%s'\n", line);
			e.printStackTrace();
			}

		return dataset;
		}

	}