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
package smile.wide.hive;

import java.util.ArrayList;

import org.antlr.runtime.CommonToken;
import org.antlr.runtime.Token;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;



/** A helper class to make posterior UDFs easier to use. When 3rd and subsequent arguments passed
 * to the UDFs are columns, the hook will rewrite the AST by adding constant strings with column 
 * names. The shortened expressions should be more readable: instead of 
 *   Posteriors('net.xdsl', null, "age", age, "reliability", reliability) 
 * it's enough to write
 *   Posteriors('net.xdsl', null, age, reliability)
 * The hook assumes column names match the node ids (comparisons are case-insensitive)
 * To enable the hook, set the Hive variable before invoking posteriors UDFs
 * set hive.semantic.analyzer.hook=smile.wide.hive.PosteriorsSemanticHook;
 * or edit the hive-site.xml.
 * Note: the analyzer hooks are described as "limited private and evolving" in Hive docs.  
 * @author shooltz@shooltz.com
  */
public class PosteriorsSemanticHook extends AbstractSemanticAnalyzerHook {
	/* (non-Javadoc)
	 * @see org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook#preAnalyze(org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext, org.apache.hadoop.hive.ql.parse.ASTNode)
	 */
	@Override
	public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast) throws SemanticException {
		// System.out.println("in preAnalyze, TOK_FUNCTION=" + HiveParser.TOK_FUNCTION);
		ast = super.preAnalyze(context, ast); 
		posteriorsFound = false;
		ASTNode newTree = copyTree(ast);
		if (posteriorsFound) {
			System.out.println("PosteriorsSemanticHook: returning new tree from preAnalyze");
			//counter = 0;
			//printNode(ast, 1);
			ast = newTree;
			//counter = 0;
			//printNode(ast, 1);
		}
		return ast;
	}
	
	/** prints the syntax tree just for debug purposes
	 */
	@SuppressWarnings("unused")
	private void printNode(Node node, int indent) {
		if (node != null) {
			String spaces = Integer.toString(counter ++);
			for (int i = 0; i < indent; i ++) {
				spaces += "  ";
			}

			if (node instanceof ASTNode) {
				ASTNode anode = (ASTNode)node;
				String s = anode.toString();
				Token t = anode.getToken();
				if (t != null) {
					s += "; t=" + t;
					s += "; TYPE=" + anode.getType();
				}
				System.out.println(spaces + s);
			}
			
			if (node.getChildren() != null) {
				for (Node c: node.getChildren()) {
					printNode(c, 1 + indent);
				}
			}
		}
	}

	/** the actual identifier values in the AST received by the hook
	 * are different from these in HiveParser.
	 */
	private static final int MAGIC_TOK_FUNCTION = 341;
	private static final int MAGIC_TOK_TABLE_OR_COLUMN = 487;
	private static final int MAGIC_TOK_STRING_CONST = 260;

	/** the actual names used by Hive users are specified by 
	 * CREATE [TEMPORARY] FUNCTION statement. It is assumed that
	 * these names will start with the value specified by this field 
	 */
	private static final String POSTERIORS_UDF_NAME_CORE = "posteriors";
	
	/** returns true if the ASTNode represents the posteriors function
	 * having only columns as evidence inputs (as opposed to constant/column pairs) 
	 */
	private boolean isPosteriorsToRewrite(ASTNode node) {
		Token t = node.getToken();
		if (t != null && t.getType() == MAGIC_TOK_FUNCTION) {
			ArrayList<Node> children = node.getChildren();
			ASTNode funcName = (ASTNode)children.get(0);
			if (funcName.getToken().getText().toLowerCase().startsWith(POSTERIORS_UDF_NAME_CORE)) {
				for (int i = 3; i < children.size(); i ++) {
					ASTNode child = (ASTNode)children.get(i);
					if (child.getType() != MAGIC_TOK_TABLE_OR_COLUMN) {
						return false;
					}
				}
				
				return true;
			}
		}
		return false;
	}
	
	private static ASTNode createNode(int type, String text) {
		return new ASTNode(new CommonToken(type, text));
	}
	
	private static ASTNode copyNode(ASTNode node) {
		return new ASTNode(new CommonToken(node.getType(), node.getText()));
	}
	
	/** rewrites the AST branch representing the posteriors UDF  
	 */
	private ASTNode rewritePosteriors(ASTNode node) {
		ASTNode newPosteriors = copyNode(node);
		ArrayList<Node> children = node.getChildren();
		ASTNode funcName = (ASTNode)children.get(0);
		ASTNode network = (ASTNode)children.get(1);
		ASTNode targets = (ASTNode)children.get(2);
		
		newPosteriors.addChild(funcName);
		newPosteriors.addChild(network);
		newPosteriors.addChild(targets);
		for (int i = 3; i < children.size(); i ++) {
			ASTNode child = (ASTNode)children.get(i);
			String childName = child.getChild(0).getText();
			newPosteriors.addChild(createNode(MAGIC_TOK_STRING_CONST, "'" + childName + "'"));
			newPosteriors.addChild(child);
		}
		return newPosteriors;
	}
	
	/** copies the entire AST. It's not possible to replace only the posteriors
	 * branch; the setChild/deleteChild throw NoSuchMethodException.
	 */
	private ASTNode copyTree(ASTNode node) {
		if (node == null) {
			return null;
		}
		ASTNode newNode = createNode(node.getType(), node.getText());
		ArrayList<Node> children = node.getChildren();
		if (children != null) {
			for (Node child: children) {
				ASTNode achild = (ASTNode)child;
				if (isPosteriorsToRewrite(achild)) {
					posteriorsFound = true;
					achild = rewritePosteriors(achild);
				}
				newNode.addChild(copyTree(achild));
			}
		}
		return newNode;
	}

	
	private int counter;
	private boolean posteriorsFound; 
}
