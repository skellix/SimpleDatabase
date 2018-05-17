package com.skellix.database.table.query.node;

import com.skellix.database.session.Session;
import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class NotQueryNode extends QueryNode {

	private QueryNode nextNode;

	@Override
	public NotQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		int index = replaceNode.parent.children.indexOf(replaceNode);
		String label = replaceNode.getLabel();
		
		TreeNode nextNode = replaceNode.getNextSibling();
		
		if (nextNode == null || !(nextNode instanceof QueryNode)) {
			
			String errorString = String.format("ERROR: expected boolean or variable after '%s' at %d, %d"
					, label, replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		replaceNode.parent.children.remove(replaceNode);
		compare((QueryNode) nextNode);
		replaceNode.parent.children.add(index, this);
		
		nextNode.parent.children.remove(nextNode);
		
		copyValuesFrom(replaceNode);
		
		resultType = Boolean.class;
		
		return this;
	}
	
	private NotQueryNode compare(QueryNode nextNode) {
		
		this.nextNode = nextNode;
		
		this.children.clear();
		this.children.add(nextNode);
		return this;
	}

	@Override
	public Object query(Session session) throws Exception {
		
		Object arg = nextNode.query(session);
		
		if (arg instanceof Boolean) {
			
			Boolean value = (Boolean) arg;
			return !value;
			
		} else {
			
			throw new Exception("expected number after !");
		}
	}

	@Override
	public String generateCode() {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("! ");
		sb.append(nextNode.generateCode());
		
		return sb.toString();
	}

}
