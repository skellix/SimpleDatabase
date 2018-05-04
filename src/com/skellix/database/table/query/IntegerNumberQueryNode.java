package com.skellix.database.table.query;

import treeparser.TreeNode;

public class IntegerNumberQueryNode extends NumberQueryNode {

	public IntegerNumberQueryNode(TreeNode from) throws NumberFormatException {
		
		copyValuesFrom(from);
		Integer.parseInt(getLabel());
	}

	@Override
	public String generateCode() {
		
		return getLabel();
	}

}
