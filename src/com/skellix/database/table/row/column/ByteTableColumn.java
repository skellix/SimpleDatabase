package com.skellix.database.table.row.column;

public class ByteTableColumn extends TableColumn<Byte> {

	@Override
	public Byte get() {
		
		return row.getByte(offset);
	}

	@Override
	public void set(Byte value) {
		
		row.setByte(offset, value);
	}

}
