package com.skellix.database.table.query.node;

import com.skellix.database.session.Session;
import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public abstract class QueryNode extends TreeNode {
	
	public Class<?> resultType = Void.class;
	
	public abstract QueryNode parse(TreeNode replaceNode) throws QueryParseException;
	public abstract Object query(Session session) throws Exception;
	public abstract String generateCode();
	
	public void wrap(TreeNode node) {
		
		copyValuesFrom(node);
		parent = node.parent;
	}
	
	@Override
	public String toString() {
		
		return generateCode();
	}

}
