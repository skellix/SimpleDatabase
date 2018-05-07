package com.skellix.database.table.query;

import com.skellix.database.session.Session;

import treeparser.TreeNode;

public abstract class QueryNode extends TreeNode {
	
	public abstract Object query(Session session) throws Exception;
	public abstract String generateCode();

}
