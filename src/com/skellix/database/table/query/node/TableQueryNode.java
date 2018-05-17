package com.skellix.database.table.query.node;

import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.skellix.database.session.Session;
import com.skellix.database.table.ExperimentalTable;
import com.skellix.database.table.RowFormat;
import com.skellix.database.table.RowFormatter;
import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class TableQueryNode extends QueryNode {
	
	private QueryNode tableIdQuery;
	
	public TableQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		TreeNode nextNode = replaceNode.getNextSibling();
		
		if (nextNode == null || !(nextNode instanceof StringQueryNode)) {
			
			String errorString = String.format("ERROR: expected string after '%s' in %d, %d"
					, replaceNode.getLabel(), replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		getTable((QueryNode) nextNode);
		copyValuesFrom(replaceNode);
		parent = replaceNode.parent;
		replaceNode.replaceWith(this);
		
		nextNode.removeFromParent();
		resultType = ExperimentalTable.class;
		
		return this;
	}

	public TableQueryNode getTable(QueryNode tableIdQuery) {
		
		this.tableIdQuery = tableIdQuery;
		
		this.children.clear();
		this.children.add(tableIdQuery);
		return this;
	}

	@Override
	public Object query(Session session) throws Exception {
		
		Object tableId = tableIdQuery.query(session);
		if (tableId instanceof String) {
			
			String tableIdString = (String) tableId;
			try {
				
				Path directory = session.getStartDirectory().resolve(tableIdString);
				Path formatPath = ExperimentalTable.getFormatPath(directory);
				RowFormat rowFormat = RowFormatter.parse(formatPath);
				ExperimentalTable table = ExperimentalTable.getOrCreate(directory, rowFormat);
				
				session.variables.put(Paths.get(tableIdString).getFileName().toString(), table);
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
