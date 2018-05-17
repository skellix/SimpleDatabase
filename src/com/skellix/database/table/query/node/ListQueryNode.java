package com.skellix.database.table.query.node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.skellix.database.session.Session;
import com.skellix.database.table.query.exception.QueryParseException;
import com.skellix.database.table.query.type.ListType;

import treeparser.TreeNode;

public class ListQueryNode extends QueryNode {

	private QueryNode[] list = new QueryNode[0];

	@Override
	public QueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		if (replaceNode.hasParent()) {
		
			TreeNode parent = replaceNode.parent;
			String enterLabel = parent.getEnterLabel();
			
			if (enterLabel != null && enterLabel.equals("{")) {
				
				return null;
			}
		}
		
		TreeNode previousNode = replaceNode.getPreviousSibling();
		
		if (previousNode == null) {
			
			String errorString = String.format("ERROR: expected value before '%s' at %d, %d"
					, replaceNode.getLabel(), replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		TreeNode nextNode = replaceNode.getNextSibling();
		
		if (nextNode == null) {
			
			String errorString = String.format("ERROR: expected value after '%s' at %d, %d"
					, replaceNode.getLabel(), replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		previousNode.removeFromParent();
		nextNode.removeFromParent();
		
		if (!(previousNode instanceof QueryNode)) {
			
			previousNode = new ValueQueryNode().parse(previousNode);
		}
		
		if (!(nextNode instanceof QueryNode)) {
			
			nextNode = new ValueQueryNode().parse(nextNode);
		}
		
		setList((QueryNode) previousNode, (QueryNode) nextNode);
		
		replaceNode.replaceWith(this);
		copyValuesFrom(replaceNode);
		
		children.add(previousNode);
		children.add(nextNode);
		
		resultType = ListType.class;
		
		return this;
	}

	private void setList(QueryNode ... list) {
		this.list = list;
	}

	@Override
	public Object query(Session session) throws Exception {
		
		List<Object> result = new ArrayList<>();
		
		for (QueryNode item : list) {
			
			Object value = item.query(session);
			
			if (value instanceof Collection) {
				
				Collection<?> list = (Collection<?>) value;
				result.addAll(list);
				
			} else {
				
				result.add(value);
			}
		}
		
		return result;
	}

	@Override
	public String generateCode() {
		// TODO Auto-generated method stub
		return null;
	}

}
