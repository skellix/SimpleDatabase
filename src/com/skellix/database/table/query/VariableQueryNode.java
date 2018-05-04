package com.skellix.database.table.query;

import treeparser.TreeNode;

public class VariableQueryNode extends QueryNode {
	
	public VariableQueryNode(TreeNode from) throws VariableFormatException {
		
		copyValuesFrom(from);
		String label = getLabel();
		for (int i = 0 ; i < label.length() ; i ++) {
			
			if (!Character.isJavaIdentifierPart(label.charAt(i))) {
				
				throw new VariableFormatException(String.format("the variable format '%s' is not valid"));
			}
		}
	}

	@Override
	public String generateCode() {
		
		return getLabel();
	}

}
