package com.skellix.database.table.query;

import java.util.AbstractMap.SimpleEntry;

import treeparser.TreeNode;

public class EntryQueryNode extends QueryNode {
	
	private StringQueryNode key;
	private QueryNode value;
	
	public static EntryQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		int index = replaceNode.parent.children.indexOf(replaceNode);
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
		
		replaceNode.parent.children.remove(replaceNode);
		EntryQueryNode queryNode = new EntryQueryNode().createMapping(key, value);
		replaceNode.parent.children.add(index, queryNode);
		
		previousNode.parent.children.remove(previousNode);
		nextNode.parent.children.remove(nextNode);
		
		queryNode.copyValuesFrom(replaceNode);
		
		return queryNode;
	}

	public EntryQueryNode createMapping(StringQueryNode key, QueryNode value) {
		
		this.key = key;
		this.value = value;
		return this;
	}

	@Override
	public Object query() throws Exception {
		
		return new SimpleEntry<StringQueryNode, QueryNode>(key, value);
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
