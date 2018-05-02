package com.skellix.database.table.row.column;

public class BooleanTableColumn extends TableColumn<Boolean> {

	@Override
	public Boolean get() {
		
		return row.getBoolean(offset);
	}

	@Override
	public void set(Boolean value) {
		
		row.setBoolean(offset, value);
	}

}
