package com.skellix.database.table.query.node;

import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public abstract class LeftRightOperatorQueryNode extends QueryNode {
	
	public static LeftRightOperatorQueryNode parse(TreeNode replaceNode, Class<? extends LeftRightOperatorQueryNode> withType) throws QueryParseException {
		
		int index = replaceNode.parent.children.indexOf(replaceNode);
		String label = replaceNode.getLabel();
		
		TreeNode previousNode = replaceNode.getPreviousSibling();
		
		if (previousNode == null) {
			
			String errorString = String.format("ERROR: expected number or variable before '%s' at %d, %d"
					, label, replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		TreeNode nextNode = replaceNode.getNextSibling();
		
		if (nextNode == null) {
			
			String errorString = String.format("ERROR: expected number or variable after '%s' at %d, %d"
					, label, replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		try {
			replaceNode.parent.children.remove(replaceNode);
			LeftRightOperatorQueryNode queryNode = withType.newInstance();
			replaceNode.parent.children.add(index, queryNode);
			
			previousNode.parent.children.remove(previousNode);
			nextNode.parent.children.remove(nextNode);
			
			queryNode.copyValuesFrom(replaceNode);
			
			queryNode.add(previousNode);
			queryNode.add(nextNode);
			
			return queryNode;
			
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public abstract String getOperatorString();

	@Override
	public String generateCode() {
		
		StringBuilder sb = new StringBuilder();
		
		TreeNode leftNode = children.get(0);
		
		if (leftNode instanceof NumberQueryNode) {
			
			QueryNode leftValue = (QueryNode) leftNode;
			sb.append(leftValue.generateCode());
		}
		
		sb.append(getOperatorString());
		
		TreeNode rightNode = children.get(1);
		
		if (rightNode instanceof NumberQueryNode) {
			
			QueryNode leftValue = (QueryNode) rightNode;
			sb.append(leftValue.generateCode());
		}
		
		return sb.toString();
	}
	
	@Override
	public String toString() {
		
		return generateCode();
	}

}
