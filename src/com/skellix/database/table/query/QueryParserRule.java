package com.skellix.database.table.query;

import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public interface QueryParserRule {

	public void parse(TreeNode tree) throws QueryParseException;
}
