package com.skellix.database.table.query;

import java.io.FileNotFoundException;

import com.skellix.database.session.Session;
import com.skellix.database.table.ExperimentalTable;

import treeparser.TreeNode;

public class TableQueryNode extends QueryNode {
	
	private QueryNode tableIdQuery;
	
	public static TableQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		int index = replaceNode.parent.children.indexOf(replaceNode);
		String label = replaceNode.getLabel();
		
		TreeNode nextNode = replaceNode.getNextSibling();
		
		if (nextNode == null || !(nextNode instanceof StringQueryNode)) {
			
			String errorString = String.format("ERROR: expected string after '%s' in %d, %d"
					, replaceNode.getLabel(), replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		TableQueryNode queryNode = new TableQueryNode().getTable((QueryNode) nextNode);
		queryNode.parent = replaceNode.parent;
		
		replaceNode.parent.children.remove(index);
		replaceNode.parent.children.add(index, queryNode);
		
		nextNode.parent.children.remove(nextNode);
		
		return queryNode;
	}

	public TableQueryNode getTable(QueryNode tableIdQuery) {
		
		this.tableIdQuery = tableIdQuery;
		return this;
	}

	@Override
	public Object query(Session session) throws Exception {
		
		Object tableId = tableIdQuery.query(session);
		if (tableId instanceof String) {
			
			String tableIdString = (String) tableId;
			try {
				ExperimentalTable table = ExperimentalTable.getById(tableIdString);
				return table;
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	@Override
	public String generateCode() {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("table ");
		sb.append(tableIdQuery.generateCode());
		
		return sb.toString();
	}

}
