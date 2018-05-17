package com.skellix.database.table.query.node;

import com.skellix.database.session.Session;
import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class IntegerNumberQueryNode extends NumberQueryNode {
	
	private int value;

	public IntegerNumberQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		try {
		
			wrap(replaceNode);
			replaceNode.replaceWith(this);
			value = Integer.parseInt(getLabel());
			resultType = Integer.class;
			return this;
			
		} catch (NumberFormatException e) {
			
			String errorString = String.format("ERROR: unable to parse number '%s' at %d, %d"
					, replaceNode.getLabel(), replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
	}
	
	@Override
	public Object query(Session session) throws Exception {
		
		return value;
	}

	@Override
	public String generateCode() {
		
		return Long.toString(value);
	}

}
