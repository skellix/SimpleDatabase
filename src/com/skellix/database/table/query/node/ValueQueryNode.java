package com.skellix.database.table.query.node;

import com.skellix.database.session.Session;
import com.skellix.database.table.query.exception.QueryParseException;
import com.skellix.database.table.query.type.ValueType;

import treeparser.TreeNode;

public class ValueQueryNode extends QueryNode {

	@Override
	public QueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		replaceNode.replaceWith(this);
		copyValuesFrom(replaceNode);
		parent = replaceNode.parent;
		
		resultType = ValueType.class;
		
		return this;
	}

	@Override
	public Object query(Session session) throws Exception {
		
		return getLabel();
	}

	@Override
	public String generateCode() {
		
		return getLabel();
	}

}
