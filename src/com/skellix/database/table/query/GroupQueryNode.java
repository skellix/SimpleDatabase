package com.skellix.database.table.query;

import treeparser.TreeNode;

public class GroupQueryNode extends QueryNode {

	public GroupQueryNode(TreeNode from) {
		
		copyValuesFrom(from);
	}

	@Override
	public String generateCode() {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append('[');
		
		TreeNode last = null;
		for (TreeNode childNode : children) {
			if (last != null) {
				
				sb.append(", ");
			}
			last = childNode;
			if (childNode instanceof QueryNode) {
				
				QueryNode child = (QueryNode) childNode;
				sb.append(child.generateCode());
			}
		}
		
		sb.append(']');
		
		return sb.toString();
	}
	
	@Override
	public String toString() {
		
		return generateCode();
	}

}