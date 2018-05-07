package com.skellix.database.table.query;

import java.nio.MappedByteBuffer;

import com.skellix.database.session.Session;

import treeparser.TreeNode;
import treeparser.io.IOSource;

public class StringQueryNode extends QueryNode {
	
	public static StringQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		int index = replaceNode.parent.children.indexOf(replaceNode);
		String label = replaceNode.getLabel();
		
		String withoutQuotes = label.substring(1, label.length() - 1);
		
		StringQueryNode queryNode = new StringQueryNode().ofString(withoutQuotes);
		queryNode.parent = replaceNode.parent;
		
		replaceNode.parent.children.remove(index);
		replaceNode.parent.children.add(index, queryNode);
		
		return queryNode;
	}
	
	public StringQueryNode ofString(String str) throws QueryParseException {
		
		IOSource source = new IOSource(MappedByteBuffer.wrap(str.getBytes()));
		TreeNode node = new TreeNode(source, 0, (int) source.buffer.limit() - 1, 0);
		return ofNode(node);
	}
	
	public StringQueryNode ofNode(TreeNode node) {
		
		copyValuesFrom(node);
		return this;
	}

	@Override
	public Object query(Session session) throws Exception {
		
		return getLabel();
	}

	@Override
	public String generateCode() {
		
		return getLabel();
	}

}
