package com.skellix.database.table.query.nodes;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.skellix.database.table.query.exception.QueryParseException;
import com.skellix.database.table.query.node.StringQueryNode;

import treeparser.TreeNode;
import treeparser.TreeParser;

class TestStringQueryNode {

	@Test
	void test() {

		String text = "\"test\"";
		TreeNode root = TreeParser.parse(text);
		TreeNode treeNode = root.children.get(0);
		try {
			StringQueryNode queryNode = new StringQueryNode().parse(treeNode);
			assertTrue(queryNode.resultType == String.class);
		} catch (QueryParseException e) {
			e.printStackTrace();
		}
	}

}
