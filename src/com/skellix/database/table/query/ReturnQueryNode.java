package com.skellix.database.table.query;

import treeparser.TreeNode;

public class ReturnQueryNode extends QueryNode {

	public ReturnQueryNode(TreeNode from) {
		
		copyValuesFrom(from);
	}

	@Override
	public String generateCode() {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("return ");
		
		TreeNode leftNode = children.get(0);
		
		if (leftNode instanceof QueryNode) {
			
			QueryNode leftValue = (QueryNode) leftNode;
			sb.append(leftValue.generateCode());
		}
		
		sb.append(";");
		
		return sb.toString();
	}
	
	@Override
	public String toString() {
		
		return generateCode();
	}

}
