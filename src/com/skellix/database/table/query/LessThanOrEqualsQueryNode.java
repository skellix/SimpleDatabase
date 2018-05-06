package com.skellix.database.table.query;

public class LessThanOrEqualsQueryNode extends LeftRightOperatorQueryNode {

	@Override
	public String getOperatorString() {
		
		return " <= ";
	}

	@Override
	public Object query() throws Exception {
		
		QueryNode leftNode = (QueryNode) children.get(0);
		QueryNode rightNode = (QueryNode) children.get(1);
		
		Object leftResultObj = leftNode.query();
		
		if (leftResultObj instanceof Number) {
			
			Number leftResult = (Number) leftResultObj;
			
			Object rightResultObj = rightNode.query();
			
			if (rightResultObj instanceof Number) {
				
				Number rightResult = (Number) rightResultObj;
				
				return leftResult.doubleValue() <= rightResult.doubleValue();
			}
		}
		
		return null;
	}

}
