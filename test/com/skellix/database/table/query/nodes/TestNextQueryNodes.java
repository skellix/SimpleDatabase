package com.skellix.database.table.query.nodes;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.skellix.database.table.query.QueryNodeParser;
import com.skellix.database.table.query.exception.QueryParseException;
import com.skellix.database.table.query.node.NextQueryNodes;
import com.skellix.database.table.query.node.QueryNode;

import treeparser.TreeNode;

class TestNextQueryNodes {

	@Test
	void test() {
//		TreeNode root = TreeParser.parse("1 0 0");
		try {
			TreeNode query = QueryNodeParser.parseTree("1 0 0");
			TreeNode queryNode = query.getFirstChild();
			List<TreeNode> nodes = NextQueryNodes.getTypes(queryNode, TreeNode.class, TreeNode.class);
			System.out.println("");
		} catch (QueryParseException e) {
			fail(e);
		}
//		TreeNode treeNode = root.children.get(0);
//		try {
//			List<TreeNode> nodes = NextQueryNodes.getTypes(treeNode, TreeNode.class, TreeNode.class);
//			System.out.println("");
//		} catch (QueryParseException e) {
//			e.printStackTrace();
//		}
	}

}
