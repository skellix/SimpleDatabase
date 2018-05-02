package com.skellix.database.table.row.column;

public class ObjectTableColumn extends TableColumn<Object> {

	@Override
	public Object get() {
		
		return row.getObject(offset);
	}

	@Override
	public void set(Object value) {
		
		row.setObject(offset, value);
	}

}
