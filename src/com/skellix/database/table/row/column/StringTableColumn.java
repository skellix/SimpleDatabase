package com.skellix.database.table.row.column;

public class StringTableColumn extends TableColumn<String> {

	@Override
	public String get() {
		
		return row.getString(offset);
	}

	@Override
	public void set(String value) {
		
		row.setString(offset, value);
	}

}
