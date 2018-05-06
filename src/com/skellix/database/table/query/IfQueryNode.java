package com.skellix.database.table.query;

import treeparser.TreeNode;

public class IfQueryNode extends QueryNode {

	@Override
	public String generateCode() {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("if (");
		
		TreeNode leftNode = children.get(0);
		
		if (leftNode instanceof QueryNode) {
			
			QueryNode leftValue = (QueryNode) leftNode;
			sb.append(leftValue.generateCode());
		}
		
		sb.append(") {");
		
		TreeNode rightNode = children.get(1);
		
		if (rightNode instanceof QueryNode) {
			
			QueryNode leftValue = (QueryNode) rightNode;
			sb.append(leftValue.generateCode());
		}
		
		sb.append("}");
		
		return sb.toString();
	}
	
	@Override
	public String toString() {
		
		return generateCode();
	}

}