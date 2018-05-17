package com.skellix.database.table;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.skellix.database.row.RowFormat;
import com.skellix.database.row.TableRow;
import com.skellix.database.session.Session;
import com.skellix.database.table.query.node.QueryNode;

public class FilteredTable extends ExperimentalTable {
	
	ExperimentalTable source = null;
	private QueryNode clause;
	private Session session;
	
	public FilteredTable() {
		//
	}

	public FilteredTable filter(RowFormat rowFormat) {
		
		this.rowFormat = rowFormat;
		return this;
	}

	public void setSource(String name, ExperimentalTable table) {
		
		source = table;
	}

	public void setClause(QueryNode clause) {
		
		this.clause = clause;
	}
	
	public void setSession(Session session) {
		
		this.session = session;
	}
	
	@Override
	public String getName() {
		
		return source.getName();
	}
	
	@Override
	public TableRow addRow(Session session) {
		
		return new CombinedTableRow().map(rowFormat);
	}
	
	@Override
	public Stream<TableRow> stream() {
		
		String tableName = getName();
		
		Map<String, Object> tableMap = new LinkedHashMap<>();
		
		return source.stream()
				.filter(row -> {
					
					for (String column : row.rowFormat.columnNames) {
						
						int columnIndex = row.rowFormat.columnIndexes.get(column);
						tableMap.put(column, row.columns.get(columnIndex).get());
					}
					
					session.variables.put(tableName, tableMap);
					
					try {
						Object result = clause.query(session);
						return result instanceof Boolean && (Boolean) result;
					} catch (Exception e) {
						e.printStackTrace();
						return false;
					}
				});
	}

}
