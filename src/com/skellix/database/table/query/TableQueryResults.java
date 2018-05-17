package com.skellix.database.table.query;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.skellix.database.table.ExperimentalTable;
import com.skellix.database.table.RowFormat;
import com.skellix.database.table.TableFormat;

@SuppressWarnings("restriction")
public class TableQueryResults {

	public Stream<Object> results;
	public Set<String> columnLabels;

	public TableQueryResults(Stream<Object> results, Set<String> columnLabels) {
		
		this.results = results;
		this.columnLabels = columnLabels;
	}
	
	public Stream<List<Entry<String, Object>>> stream(ExperimentalTable table) {
		
		RowFormat rowFormat = table.rowFormat;
		
		return results.map(result -> {
			
//			if (result instanceof ScriptObjectMirror) {
//				
//				ScriptObjectMirror sciptObject = (ScriptObjectMirror) result;
//				if (sciptObject.isArray()) {
//					
//					return columnLabels.stream().map(key -> {
//						
//						Integer byteSize = rowFormat.columnSizes.get(key);
//						ColumnType columnType = rowFormat.columnTypes.get(key);
//						String formatString = columnType.formatString(byteSize);
//						
//						return sciptObject.entrySet().stream()
//							.map(entry -> {
//								
//								Object o = entry.getValue();
//								
//								if (o instanceof ScriptObjectMirror) {
//									
//									ScriptObjectMirror child = (ScriptObjectMirror) o;
//									Object value = child.get(key);
//									return value;
//								}
//								return null;
//								
//							})
//							.filter(value -> value != null)
//							.map(value -> new SimpleEntry<>(formatString, value))
//							.findFirst().orElse(null);
//						
//					}).collect(Collectors.toList());
//				}
//			}
			
			return Arrays.asList();
		});
	}
	
	public Collector<CharSequence, ?, String> getJoiningString(TableFormat tableFormat) {
		
		switch (tableFormat) {
		case FORMAT_CELLS: return Collectors.joining("|");
		case FORMAT_CSV: return Collectors.joining("|");
		}
		return null;
	}

	public void printResults(ExperimentalTable table, TableFormat tableFormat) {
		
		Stream<List<Entry<String, Object>>> stream = stream(table);
		
		Collector<CharSequence, ?, String> joining = getJoiningString(tableFormat);
		
		stream.forEach(row -> {
			
			String str = row.stream().map(entry -> {
				
				return String.format(entry.getKey(), entry.getValue());
				
			}).collect(joining);
			
			switch (tableFormat) {
			case FORMAT_CELLS: System.out.println('|' + str + '|'); break;
			case FORMAT_CSV: System.out.println(str); break;
			}
		});
	}

}
