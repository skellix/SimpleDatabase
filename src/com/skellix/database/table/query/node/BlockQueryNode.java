package com.skellix.database.table.query.node;

import com.skellix.database.session.Session;
import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class BlockQueryNode extends QueryNode {
	
	private QueryNode queryNode = null;

	public BlockQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		replaceNode.replaceWith(this);
		parent = replaceNode.parent;
		
		TreeNode lastChild = null;
		
		for (TreeNode child : replaceNode.children) {
			
			if (!child.getLabel().equals(",")) {
				
				children.add(child);
				lastChild = child;
			}
		}
		
		if (lastChild == null || !(lastChild instanceof QueryNode)) {
			
			resultType = Void.class;
			
		} else {
			
			queryNode = (QueryNode) lastChild;
			resultType = queryNode.resultType;
		}
		
		return this;
	}

	@Override
	public Object query(Session session) throws Exception {
		
		if (queryNode != null) {
			
			return queryNode.query(session);
		}
		
		return null;
	}
	
	@Override
	public String generateCode() {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append('(');
		sb.append(queryNode.generateCode());
		sb.append(')');
		
		return sb.toString();
	}

}
