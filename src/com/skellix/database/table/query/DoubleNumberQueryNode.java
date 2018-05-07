package com.skellix.database.table.query;

import treeparser.TreeNode;

public class DoubleNumberQueryNode extends NumberQueryNode {
	
	public static DoubleNumberQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		int index = replaceNode.getIndex();
		String label = replaceNode.getLabel();
		
		try {
		
			DoubleNumberQueryNode queryNode = new DoubleNumberQueryNode(replaceNode);
			replaceNode.parent.children.remove(index);
			replaceNode.parent.children.add(index, queryNode);
			
			return queryNode;
			
		} catch (NumberFormatException e) {
			
			String errorString = String.format("ERROR: unable to parse number '%s' at %d, %d"
					, label, replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
	}

	public DoubleNumberQueryNode(TreeNode from) throws NumberFormatException {
		
		copyValuesFrom(from);
		Double.parseDouble(getLabel());
	}

	@Override
	public String generateCode() {
		
		return getLabel();
	}

}
