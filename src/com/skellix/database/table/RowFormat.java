package com.skellix.database.table;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

}
