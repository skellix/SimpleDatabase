package com.skellix.database.table.query;

import java.nio.MappedByteBuffer;

import treeparser.TreeNode;
import treeparser.io.IOSource;

public class StringQueryNode extends QueryNode {
	
	public StringQueryNode ofString(String str) {
		
		IOSource source = new IOSource(MappedByteBuffer.wrap(str.getBytes()));
		TreeNode node = new TreeNode(source, 0, (int) source.buffer.limit() - 1, 0);
		return ofNode(node);
	}
	
	public StringQueryNode ofNode(TreeNode node) {
		
		copyValuesFrom(node);
		return this;
	}

	@Override
	public Object query() throws Exception {
		
		return getLabel();
	}

	@Override
	public String generateCode() {
		
		return getLabel();
	}

}
