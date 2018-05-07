package com.skellix.database.table.query;

import com.skellix.database.session.Session;

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

	@Override
	public Object query(Session session) throws Exception {
		
		QueryNode leftNode = (QueryNode) children.get(0);
		QueryNode rightNode = (QueryNode) children.get(1);
		
		Object testResultObj = leftNode.query(session);
		
		if (testResultObj instanceof Boolean) {
			
			Boolean testResult = (Boolean) testResultObj;
			
			if (testResult) {
				
				return rightNode.query(session);
			}
		}
		
		return null;
	}

}
