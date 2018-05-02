package com.skellix.database.table;

import com.skellix.database.table.row.column.BooleanTableColumn;
import com.skellix.database.table.row.column.ByteArrayTableColumn;
import com.skellix.database.table.row.column.ByteTableColumn;
import com.skellix.database.table.row.column.CharTableColumn;
import com.skellix.database.table.row.column.DoubleTableColumn;
import com.skellix.database.table.row.column.FloatTableColumn;
import com.skellix.database.table.row.column.IntTableColumn;
import com.skellix.database.table.row.column.LongTableColumn;
import com.skellix.database.table.row.column.ObjectTableColumn;
import com.skellix.database.table.row.column.StringTableColumn;
import com.skellix.database.table.row.column.TableColumn;

public enum ColumnType {
	
	BOOLEAN,
	BYTE,
	CHAR,
	INT,
	LONG,
	FLOAT,
	DOUBLE,
	STRING,
	BYTE_ARRAY,
	OBJECT;

	public static ColumnType parse(String type) {
		
		if (type.toUpperCase().equals("BOOLEAN")) return BOOLEAN;
		if (type.toUpperCase().equals("BYTE")) return BYTE;
		if (type.toUpperCase().equals("CHAR")) return CHAR;
		if (type.toUpperCase().equals("INT")) return INT;
		if (type.toUpperCase().equals("LONG")) return LONG;
		if (type.toUpperCase().equals("FLOAT")) return FLOAT;
		if (type.toUpperCase().equals("DOUBLE")) return DOUBLE;
		if (type.toUpperCase().equals("STRING")) return STRING;
		if (type.toUpperCase().equals("BYTE_ARRAY")) return BYTE_ARRAY;
		if (type.toUpperCase().equals("OBJECT")) return OBJECT;
		return null;
	}

	public TableColumn newInstance(TableRow tableRow, Integer offset) {
		
		switch (this) {
		case BOOLEAN: return new BooleanTableColumn().map(tableRow, offset);
		case BYTE: return new ByteTableColumn().map(tableRow, offset);
		case CHAR: return new CharTableColumn().map(tableRow, offset);
		case INT: return new IntTableColumn().map(tableRow, offset);
		case LONG: return new LongTableColumn().map(tableRow, offset);
		case FLOAT: return new FloatTableColumn().map(tableRow, offset);
		case DOUBLE: return new DoubleTableColumn().map(tableRow, offset);
		case STRING: return new StringTableColumn().map(tableRow, offset);
		case BYTE_ARRAY: return new ByteArrayTableColumn().map(tableRow, offset);
		case OBJECT: return new ObjectTableColumn().map(tableRow, offset);
		}
		return null;
	}

	public String formatString(Integer byteSize) {
		
		switch (this) {
		case BOOLEAN: return "%5b";
		case BYTE: return String.format("%%%dd", stringLength(byteSize));
		case CHAR: return "%1c";
		case INT: return String.format("%%%dd", stringLength(byteSize));
		case LONG: return String.format("%%%dd", stringLength(byteSize));
		case FLOAT: return String.format("%%%df", stringLength(byteSize));
		case DOUBLE: return String.format("%%%df", stringLength(byteSize));
		case STRING: return String.format("%%%ds", stringLength(byteSize));
		case BYTE_ARRAY: return String.format("%%%ds", stringLength(byteSize));
		case OBJECT: return String.format("%%%ds", stringLength(byteSize));
		}
		return null;
	}

	public int stringLength(Integer byteSize) {
		
		switch (this) {
		case BOOLEAN: return 5;
		case BYTE: return Math.max(Byte.toString(Byte.MIN_VALUE).length(), Byte.toString(Byte.MAX_VALUE).length());
		case CHAR: return 1;
		case INT: return Math.max(Integer.toString(Integer.MIN_VALUE).length(), Integer.toString(Integer.MAX_VALUE).length());
		case LONG: return Math.max(Long.toString(Long.MIN_VALUE).length(), Long.toString(Long.MAX_VALUE).length());
		case FLOAT: return Math.max(Float.toString(Float.MIN_VALUE).length(), Float.toString(Float.MAX_VALUE).length());
		case DOUBLE: return Math.max(Double.toString(Double.MIN_VALUE).length(), Double.toString(Double.MAX_VALUE).length());
		case STRING: return byteSize;
		case BYTE_ARRAY: return (Math.max(Byte.toString(Byte.MIN_VALUE).length(), Byte.toString(Byte.MAX_VALUE).length()) + 2) * byteSize + 1;
		case OBJECT: return byteSize;
		}
		return 4;
	}

	public int defaultByteLength() {
		
		switch (this) {
		case BOOLEAN: return 5;
		case BYTE: return Byte.BYTES;
		case CHAR: return Character.BYTES;
		case INT: return Integer.BYTES;
		case LONG: return Long.BYTES;
		case FLOAT: return Float.BYTES;
		case DOUBLE: return Double.BYTES;
		case STRING: return 32;
		case BYTE_ARRAY: return 16;
		case OBJECT: return 32;
		}
		return 0;
	}
	
}
