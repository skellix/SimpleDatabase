package com.skellix.database.table.query.node;

import com.skellix.database.session.Session;
import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class EqualsQueryNode extends QueryNode {
	
	private QueryNode previousNode;
	private QueryNode nextNode;

	public EqualsQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		TreeNode previousNode = replaceNode.getPreviousSibling();
		
		if (previousNode == null || !(previousNode instanceof QueryNode)) {
			
			String errorString = String.format("ERROR: expected number or variable before '%s' at %d, %d"
					, replaceNode.getLabel(), replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		TreeNode nextNode = replaceNode.getNextSibling();
		
		if (nextNode == null || !(nextNode instanceof QueryNode)) {
			
			String errorString = String.format("ERROR: expected number or variable after '%s' at %d, %d"
					, replaceNode.getLabel(), replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		compare((QueryNode) previousNode, (QueryNode) nextNode);
		replaceNode.replaceWith(this);
		
		previousNode.removeFromParent();
		nextNode.removeFromParent();
		
		copyValuesFrom(replaceNode);
		
		return this;
	}

	private EqualsQueryNode compare(QueryNode previousNode, QueryNode nextNode) {
		
		this.previousNode = previousNode;
		this.nextNode = nextNode;
		
		this.children.clear();
		this.children.add(previousNode);
		this.children.add(nextNode);
		return this;
	}

	@Override
	public Object query(Session session) throws Exception {
	
		Object leftArg = previousNode.query(session);
		Object rightArg = nextNode.query(session);

		return leftArg.equals(rightArg);
	}

	@Override
	public String generateCode() {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append(previousNode.generateCode()); 
		sb.append(" == ");
		sb.append(nextNode.generateCode());
		
		return sb.toString();
	}
	
	@Override
	public String toString() {
		
		return generateCode();
	}
}
