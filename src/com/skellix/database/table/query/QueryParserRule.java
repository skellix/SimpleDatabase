package com.skellix.database.table.query;

import treeparser.TreeNode;

public interface QueryParserRule {

	public void parse(TreeNode tree) throws QueryParseException;
}
