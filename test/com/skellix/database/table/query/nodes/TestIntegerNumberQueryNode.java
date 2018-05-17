package com.skellix.database.table.query.nodes;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.skellix.database.table.query.exception.QueryParseException;
import com.skellix.database.table.query.node.IntegerNumberQueryNode;
import com.skellix.database.table.query.node.StringQueryNode;

import treeparser.TreeNode;
import treeparser.TreeParser;

class TestIntegerNumberQueryNode {

	@Test
	void test() {

		String text = "0";
		TreeNode root = TreeParser.parse(text);
		TreeNode treeNode = root.children.get(0);
		try {
			IntegerNumberQueryNode queryNode = new IntegerNumberQueryNode().parse(treeNode);
			assertTrue(queryNode.resultType == Integer.class);
		} catch (QueryParseException e) {
			e.printStackTrace();
		}
	}

}
