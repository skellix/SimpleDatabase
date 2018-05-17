package com.skellix.database.table.query.node;

import com.skellix.database.session.Session;
import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class LessThanOrEqualsQueryNode extends LeftRightOperatorQueryNode {
	
	private QueryNode previousNode;
	private QueryNode nextNode;

	@Override
	public QueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		int index = replaceNode.parent.children.indexOf(replaceNode);
		String label = replaceNode.getLabel();
		
		TreeNode previousNode = replaceNode.getPreviousSibling();
		
		if (previousNode == null || !(previousNode instanceof QueryNode)) {
			
			String errorString = String.format("ERROR: expected number or variable before '%s' at %d, %d"
					, label, replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		TreeNode nextNode = replaceNode.getNextSibling();
		
		if (nextNode == null || !(nextNode instanceof QueryNode)) {
			
			String errorString = String.format("ERROR: expected number or variable after '%s' at %d, %d"
					, label, replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		compare((QueryNode) previousNode, (QueryNode) nextNode);
		replaceNode.replaceWith(this);
		
		previousNode.removeFromParent();
		nextNode.removeFromParent();
		
		copyValuesFrom(replaceNode);
		
		resultType = Boolean.class;
		
		return this;
	}
	
	private LessThanOrEqualsQueryNode compare(QueryNode previousNode, QueryNode nextNode) {
		
		this.previousNode = previousNode;
		this.nextNode = nextNode;
		
		this.children.clear();
		this.children.add(previousNode);
		this.children.add(nextNode);
		return this;
	}

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
