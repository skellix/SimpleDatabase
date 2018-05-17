package com.skellix.database.table;

import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.skellix.database.row.RowFormat;
import com.skellix.database.row.TableRow;
import com.skellix.database.table.row.column.TableColumn;

public class CombinedTableRow extends TableRow {
	
	public CombinedTableRow map(RowFormat rowFormat) {
		
		this.rowFormat = rowFormat;
		size = rowFormat.rowSize;
		offset = 0;
		buffer = MappedByteBuffer.allocate(size);
		
		@SuppressWarnings("rawtypes")
		List<TableColumn> columns = new ArrayList<>();
		
		for (String columnName : rowFormat.columnNames) {
			
			ColumnType columnType = rowFormat.columnTypes.get(columnName);
			Integer columnOffset = rowFormat.columnOffsets.get(columnName);
			columns.add(TableColumn.map(this, columnType, columnOffset));
		}
		this.columns = columns;
		return this;
	}

}
