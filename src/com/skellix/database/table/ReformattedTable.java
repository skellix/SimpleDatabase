package com.skellix.database.table;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.skellix.database.row.RowFormat;
import com.skellix.database.row.TableRow;
import com.skellix.database.session.Session;

public class ReformattedTable extends Table {
	
	Table source = null;
	private Session session;
	
	public ReformattedTable() {
		//
	}

	public ReformattedTable createFormat(RowFormat rowFormat, List<Object> columnList) throws Exception {
		
		List<String> columnNames = new LinkedList<>();
		Map<String, ColumnType> columnTypes = new LinkedHashMap<>();
		Map<String, Integer> columnSizes = new LinkedHashMap<>();
		
		for (Object column : columnList) {
			
			String columnName = column.toString();
			
			if (!rowFormat.columnNames.contains(columnName)) {
				
				throw new Exception("the table does not contain the column '" + columnName + "'");
			}
			
			if (!columnNames.contains(columnName)) {
			
				columnNames.add(columnName);
				columnTypes.put(columnName, rowFormat.columnTypes.get(columnName));
				columnSizes.put(columnName, rowFormat.columnSizes.get(columnName));
			}
		}
		
		this.rowFormat = new RowFormat(columnNames, columnTypes, columnSizes);
		return this;
	}

	public void setSource(String name, Table table) {
		
		source = table;
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
		
		return source.stream()
				.map(sourceRow -> {
					
					TableRow row = addRow(session);
					
					for (String column : rowFormat.columnNames) {
						
						int sourceColumnIndex = sourceRow.rowFormat.columnIndexes.get(column);
						int columnIndex = rowFormat.columnIndexes.get(column);
						
						Object value = sourceRow.columns.get(sourceColumnIndex).get();
						row.columns.get(columnIndex).set(value);
					}
					
					return row;
				});
	}

}
