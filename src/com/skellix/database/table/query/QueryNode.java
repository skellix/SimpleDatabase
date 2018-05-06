package com.skellix.database.table.query;

import treeparser.TreeNode;

public abstract class QueryNode extends TreeNode {
	
	public abstract Object query() throws Exception;
	public abstract String generateCode();

}
