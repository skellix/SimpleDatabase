package com.skellix.database.table.row.column;

import com.skellix.database.table.ColumnType;
import com.skellix.database.table.TableRow;

public abstract class TableColumn <T> {
	
	protected TableRow row;
	protected int offset;
	
	public abstract T get();
	public abstract void set(T value);
	
	public TableColumn<T> map(TableRow row, int offset) {
		
		this.row = row;
		this.offset = offset;
		return this;
	}

	public static TableColumn map(TableRow tableRow, ColumnType columnType, Integer offset) {
		
		return columnType.newInstance(tableRow, offset);
	}

}
