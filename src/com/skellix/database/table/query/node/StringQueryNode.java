package com.skellix.database.table.query.node;

import com.skellix.database.session.Session;
import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class StringQueryNode extends QueryNode {
	
	public StringQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		wrap(replaceNode);
		start ++;
		end --;
		
		replaceNode.replaceWith(this);
		resultType = String.class;
		
		return this;
	}
	
	@Override
	public Object query(Session session) throws Exception {
		
		return getLabel();
	}

	@Override
	public String generateCode() {
		
		return "\"" + getLabel() + "\"";
	}

}
