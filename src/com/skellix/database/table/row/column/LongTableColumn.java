package com.skellix.database.table.row.column;

public class LongTableColumn extends TableColumn<Long> {

	@Override
	public Long get() {
		
		return row.getLong(offset);
	}

	@Override
	public void set(Long value) {
		
		row.setLong(offset, value);
	}

}
