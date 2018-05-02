package com.skellix.database.table.row.column;

public class FloatTableColumn extends TableColumn<Float> {

	@Override
	public Float get() {
		
		return row.getFloat(offset);
	}

	@Override
	public void set(Float value) {
		
		row.setFloat(offset, value);
	}

}
