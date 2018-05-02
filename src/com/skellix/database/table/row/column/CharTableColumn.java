package com.skellix.database.table.row.column;

public class CharTableColumn extends TableColumn<Character> {

	@Override
	public Character get() {
		
		return row.getChar(offset);
	}

	@Override
	public void set(Character value) {
		
		row.setChar(offset, value);
	}

}
