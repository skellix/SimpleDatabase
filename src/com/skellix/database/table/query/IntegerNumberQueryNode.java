package com.skellix.database.table.query;

import com.skellix.database.session.Session;

import treeparser.TreeNode;

public class IntegerNumberQueryNode extends NumberQueryNode {
	
	public static IntegerNumberQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		int index = replaceNode.getIndex();
		String label = replaceNode.getLabel();
		
		try {
		
			IntegerNumberQueryNode queryNode = new IntegerNumberQueryNode(replaceNode);
			replaceNode.parent.children.remove(index);
			replaceNode.parent.children.add(index, queryNode);
			
			return queryNode;
			
		} catch (NumberFormatException e) {
			
			String errorString = String.format("ERROR: unable to parse number '%s' at %d, %d"
					, label, replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
	}

	public IntegerNumberQueryNode(TreeNode from) throws NumberFormatException {
		
		copyValuesFrom(from);
		Integer.parseInt(getLabel());
	}
	
	@Override
	public Object query(Session session) throws Exception {
		
		return Integer.parseInt(getLabel());
	}

	@Override
	public String generateCode() {
		
		return getLabel();
	}

}
