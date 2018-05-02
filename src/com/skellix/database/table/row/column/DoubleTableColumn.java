package com.skellix.database.table.row.column;

public class DoubleTableColumn extends TableColumn<Double> {

	@Override
	public Double get() {
		
		return row.getDouble(offset);
	}

	@Override
	public void set(Double value) {
		
		row.setDouble(offset, value);
	}

}
