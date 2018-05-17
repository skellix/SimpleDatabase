package com.skellix.database.table.query.node;

import com.skellix.database.session.Session;
import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class LongNumberQueryNode extends NumberQueryNode {
	
	private long value;

	@Override
	public LongNumberQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		try {
			
			wrap(replaceNode);
			replaceNode.replaceWith(this);
			value = Long.parseLong(getLabel());
			resultType = Long.class;
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
