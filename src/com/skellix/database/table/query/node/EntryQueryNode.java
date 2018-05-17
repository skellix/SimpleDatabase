package com.skellix.database.table.query.node;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import com.skellix.database.session.Session;
import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class EntryQueryNode extends QueryNode {
	
	private StringQueryNode key;
	private QueryNode value;
	private Entry result;
	
	public EntryQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		String label = replaceNode.getLabel();

		TreeNode previousNode = replaceNode.getPreviousSibling();
		
		if (previousNode == null || !(previousNode instanceof StringQueryNode)) {
			
			String errorString = String.format("ERROR: expected string before '%s' at %d, %d"
					, label, replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		TreeNode nextNode = replaceNode.getNextSibling();
		
		if (nextNode == null || !(nextNode instanceof QueryNode)) {
			
			String errorString = String.format("ERROR: expected node after '%s' at %d, %d"
					, label, replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		StringQueryNode key = (StringQueryNode) previousNode;
		QueryNode value = (QueryNode) nextNode;
		
		
		createMapping(key, value);
		replaceNode.replaceWith(this);
		
		previousNode.parent.children.remove(previousNode);
		nextNode.parent.children.remove(nextNode);
		
		copyValuesFrom(replaceNode);
		
		resultType = Entry.class;
		
		return this;
	}

	public EntryQueryNode createMapping(StringQueryNode key, QueryNode value) {
		
		this.key = key;
		this.value = value;
		
		this.children.clear();
		this.children.add(key);
		this.children.add(value);
		
		result = new SimpleEntry<StringQueryNode, QueryNode>(key, value);
		return this;
	}

	@Override
	public Object query(Session session) throws Exception {
		
		return result;
	}

	@Override
	public String generateCode() {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append(key.generateCode());
		sb.append(':');
		sb.append(value.generateCode());
		
		return sb.toString();
	}
	
	@Override
	public String toString() {
		
		return generateCode();
	}

}
