package com.skellix.database.table.query.node;

import com.skellix.database.session.Session;
import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class NumberQueryNode extends QueryNode {

	@Override
	public QueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		throw new QueryParseException("Found unformatted number");
	}

	@Override
	public Object query(Session session) throws Exception {
		
		return null;
	}

	@Override
	public String generateCode() {
		//
		return null;
	}

}
