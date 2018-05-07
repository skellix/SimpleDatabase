package com.skellix.database.table.query;

import com.skellix.database.session.Session;

public class LessThanOrEqualsQueryNode extends LeftRightOperatorQueryNode {

	@Override
	public String getOperatorString() {
		
		return " <= ";
	}

	@Override
	public Object query(Session session) throws Exception {
		
		QueryNode leftNode = (QueryNode) children.get(0);
		QueryNode rightNode = (QueryNode) children.get(1);
		
		Object leftResultObj = leftNode.query(session);
		
		if (leftResultObj instanceof Number) {
			
			Number leftResult = (Number) leftResultObj;
			
			Object rightResultObj = rightNode.query(session);
			
			if (rightResultObj instanceof Number) {
				
				Number rightResult = (Number) rightResultObj;
				
				return leftResult.doubleValue() <= rightResult.doubleValue();
			}
		}
		
		return null;
	}

}
