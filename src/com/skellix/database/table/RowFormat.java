package com.skellix.database.table;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/***
 * This class is used for storing the row format of a table, including:</br>
 * <ul>
 *   <li>The names of the columns</li>
 *   <li>The types of the columns using the ColumnType</li>
 *   <li>The size of the columns in bytes</li>
 *   <li>The byte offset of each column from the start of the row</li>
 * </ul>
 */
public class RowFormat {

	public List<String> columnNames;
	public Map<String, Integer> columnIndexes;
	public Map<String, ColumnType> columnTypes;
	public Map<String, Integer> columnSizes;
	public Map<String, Integer> columnOffsets;
	public int rowSize = 0;

	public RowFormat(List<String> columnNames, Map<String, ColumnType> columnTypes, Map<String, Integer> columnSizes) {
		
		this.columnNames = columnNames;
		this.columnTypes = columnTypes;
		this.columnSizes = columnSizes;
		
		columnIndexes = new LinkedHashMap<>();
		columnOffsets = new LinkedHashMap<>();
		
		int i = 0;
		
		for (String key : columnNames) {
			
			columnIndexes.put(key, i ++);
			columnOffsets.put(key, rowSize);
			rowSize += columnSizes.get(key);
		}
	}

	public void write(Path rowFormatPath) throws IOException {
		
		try (OutputStream out = Files.newOutputStream(rowFormatPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
			
			out.write(toString().getBytes());
		}
	}
	
	@Override
	public String toString() {
		
		StringBuilder sb = new StringBuilder();
		
		for (String key : columnNames) {
			
			sb.append("{");
			sb.append(columnTypes.get(key).toString());
			sb.append(" ");
			sb.append(key);
			sb.append(" ");
			sb.append(String.format("%d", columnSizes.get(key)));
			sb.append("}");
		}
		
		return sb.toString();
	}
	
	public String getInsertString() {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("{");
		
		String keysString = columnNames.stream()
				.map(key -> String.format("'%s': %s", key, columnTypes.get(key).getDefaultValueString()))
				.collect(Collectors.joining(", "));
		
		sb.append(keysString);
		sb.append("}");
		
		return sb.toString();
	}

	public void printHeader(TableFormat format) {
		
		TableFormatter.printTableStart(this, format);
	}
	
	public String getHeader(TableFormat format) {
		
		return TableFormatter.getTableStart(this, format);
	}
	
	public void printEnd(TableFormat format) {
		
		TableFormatter.printTableEnd(this, format);
	}
	
	public String getEnd(TableFormat format) {
		
		return TableFormatter.getTableEnd(this, format);
	}

}
