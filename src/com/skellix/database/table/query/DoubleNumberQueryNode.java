package com.skellix.database.table.query;

import treeparser.TreeNode;

public class DoubleNumberQueryNode extends NumberQueryNode {

	public DoubleNumberQueryNode(TreeNode from) throws NumberFormatException {
		
		copyValuesFrom(from);
		Double.parseDouble(getLabel());
	}

	@Override
	public String generateCode() {
		
		return getLabel();
	}

}
