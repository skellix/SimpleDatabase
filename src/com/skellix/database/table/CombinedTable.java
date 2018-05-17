package com.skellix.database.table;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.skellix.database.session.Session;
import com.skellix.database.table.query.node.QueryNode;

public class CombinedTable extends ExperimentalTable {
	
	Map<String, ExperimentalTable> sources = new LinkedHashMap<>();
	private QueryNode clause;
	private Session session;
	
	public CombinedTable() {
		// TODO Auto-generated constructor stub
	}

	public CombinedTable combine(RowFormat ... rowFormats) {
		
		List<String> columnNames = new LinkedList<>();
		Map<String, ColumnType> columnTypes = new LinkedHashMap<>();
		Map<String, Integer> columnSizes = new LinkedHashMap<>();
		
		for (RowFormat rowFormat : rowFormats) {
			
			for (String columnName : rowFormat.columnNames) {
				
				if (!columnNames.contains(columnName)) {
				
					columnNames.add(columnName);
					columnTypes.put(columnName, rowFormat.columnTypes.get(columnName));
					columnSizes.put(columnName, rowFormat.columnSizes.get(columnName));
				}
			}
		}
		
		this.rowFormat = new RowFormat(columnNames, columnTypes, columnSizes);
		return this;
	}

	public void setSource(String name, ExperimentalTable table) {
		
		sources.put(name, table);
	}

	public void setClause(QueryNode clause) {
		
		this.clause = clause;
	}
	
	public void setSession(Session session) {
		
		this.session = session;
	}
	
	@Override
	public String getName() {
		
		String[] tableNames = sources.keySet().toArray(new String[0]);
		String firstTableName = tableNames[0];
		String secondTableName = tableNames[1];
		return String.format("%s_%s", firstTableName, secondTableName);
	}
	
	@Override
	public TableRow addRow(Session session) {
		
		return new CombinedTableRow().map(rowFormat);
	}
	
	@Override
	public Stream<TableRow> stream() {
		
		String[] tableNames = sources.keySet().toArray(new String[0]);
		String firstTableName = tableNames[0];
		String secondTableName = tableNames[1];
		
		Map<String, Object> firstTableMap = new LinkedHashMap<>();
		Map<String, Object> secondTableMap = new LinkedHashMap<>();
		
		return sources.get(firstTableName).stream()
				.flatMap(leftRow -> {
					
					for (String column : leftRow.rowFormat.columnNames) {
						
						int columnIndex = leftRow.rowFormat.columnIndexes.get(column);
						firstTableMap.put(column, leftRow.columns.get(columnIndex).get());
					}
					
					session.variables.put(firstTableName, firstTableMap);
					
					return sources.get(secondTableName).stream()
							.filter(rightRow -> {
								
								for (String column : rightRow.rowFormat.columnNames) {
									
									int columnIndex = rightRow.rowFormat.columnIndexes.get(column);
									secondTableMap.put(column, rightRow.columns.get(columnIndex).get());
								}
								
								session.variables.put(secondTableName, secondTableMap);
								
								try {
									Object result = clause.query(session);
									return result instanceof Boolean && (Boolean) result;
								} catch (Exception e) {
									e.printStackTrace();
									return false;
								}
							})
							.map(rightRow -> {
								
								TableRow row = addRow(session);
								
								for (String column : rowFormat.columnNames) {
									
									int columnIndex = rowFormat.columnIndexes.get(column);
									
									if (firstTableMap.containsKey(column)) {
										
										Object value = firstTableMap.get(column);
										row.columns.get(columnIndex).set(value);
										
									} else {
										
										Object value = secondTableMap.get(column);
										row.columns.get(columnIndex).set(value);
									}
								}
								
								return row;
							});
				});
	}

}
