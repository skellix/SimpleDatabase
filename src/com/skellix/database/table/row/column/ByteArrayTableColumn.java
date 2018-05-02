package com.skellix.database.table.row.column;

public class ByteArrayTableColumn extends TableColumn<byte[]> {

	@Override
	public byte[] get() {
		
		return row.getBytes(offset);
	}

	@Override
	public void set(byte[] value) {
		
		row.setBytes(offset, value);
	}

}
