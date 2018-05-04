package com.skellix.database.table.query;

import treeparser.TreeNode;

public abstract class QueryNode extends TreeNode {
	
	public abstract String generateCode();

}
