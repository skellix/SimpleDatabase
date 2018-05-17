package com.skellix.database.table.query.node;

import com.skellix.database.session.Session;
import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class OrQueryNode extends QueryNode {
	
	private QueryNode previousNode;
	private QueryNode nextNode;
	
	@Override
	public QueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		int index = replaceNode.parent.children.indexOf(replaceNode);
		String label = replaceNode.getLabel();
		
		TreeNode previousNode = replaceNode.getPreviousSibling();
		
		if (previousNode == null || !(previousNode instanceof QueryNode)) {
			
			String errorString = String.format("ERROR: expected boolean or variable before '%s' at %d, %d"
					, label, replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		TreeNode nextNode = replaceNode.getNextSibling();
		
		if (nextNode == null || !(nextNode instanceof QueryNode)) {
			
			String errorString = String.format("ERROR: expected boolean or variable after '%s' at %d, %d"
					, label, replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		replaceNode.parent.children.remove(replaceNode);
		compare((QueryNode) previousNode, (QueryNode) nextNode);
		replaceNode.parent.children.add(index, this);
		
		previousNode.parent.children.remove(previousNode);
		nextNode.parent.children.remove(nextNode);
		
		copyValuesFrom(replaceNode);
		
		resultType = Boolean.class;
		
		return this;
	}
	
	private OrQueryNode compare(QueryNode previousNode, QueryNode nextNode) {
		
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
		
		if (leftArg instanceof Boolean) {
			
			Boolean leftValue = (Boolean) leftArg;
			Object rightArg = nextNode.query(session);
			
			if (rightArg instanceof Boolean) {
				
				Boolean rightValue = (Boolean) rightArg;
				
				return leftValue || rightValue;
			} else {
				
				throw new Exception("expected boolean after ||");
			}
		} else {
			
			throw new Exception("expected boolean before ||");
		}
	}

	@Override
	public String generateCode() {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append(previousNode.generateCode()); 
		sb.append(" || ");
		sb.append(nextNode.generateCode());
		
		return sb.toString();
	}

}