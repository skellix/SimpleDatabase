package com.skellix.database.table.row.column;

public class IntTableColumn extends TableColumn<Integer> {

	@Override
	public Integer get() {
		
		return row.getInt(offset);
	}

	@Override
	public void set(Integer value) {
		
		row.setInt(offset, value);
	}

}
