package com.skellix.database.table.query;

import java.io.FileNotFoundException;

import com.skellix.database.table.ExperimentalTable;

public class TableQueryNode extends QueryNode {
	
	private QueryNode tableIdQuery;

	public TableQueryNode getTable(QueryNode tableIdQuery) {
		
		this.tableIdQuery = tableIdQuery;
		return this;
	}

	@Override
	public Object query() throws Exception {
		
		Object tableId = tableIdQuery.query();
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
		// TODO Auto-generated method stub
		return null;
	}

}
