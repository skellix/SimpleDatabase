package com.skellix.database.table;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

class TestTypedRows {

	@Test
	void test() {
		
		Path directory = Paths.get("testData", "experimental");
		
		StringBuilder sb = new StringBuilder();
		sb.append("{boolean a}");
		sb.append("{byte b}");
		sb.append("{char c}");
		sb.append("{int d}");
		sb.append("{long e}");
		sb.append("{float f}");
		sb.append("{double g}");
		sb.append("{string h 32}");
		sb.append("{byte_array i 16}");
		sb.append("{object j 64}");
		
		RowFormat rowFormat = RowFormatter.parse(sb.toString());
		
		ExperimentalTable table = new ExperimentalTable(directory, rowFormat);
		try {
			table.deleteTable();
		} catch (IOException e) {
			e.printStackTrace();
		}
		table.initTable();
		TableRow row = table.addRow();
		row.columns.get(rowFormat.columnIndexes.get("a")).set(true);
		row.columns.get(rowFormat.columnIndexes.get("b")).set((byte) 1);
		row.columns.get(rowFormat.columnIndexes.get("c")).set('c');
		row.columns.get(rowFormat.columnIndexes.get("d")).set(2);
		row.columns.get(rowFormat.columnIndexes.get("e")).set(3L);
		row.columns.get(rowFormat.columnIndexes.get("f")).set(0.1f);
		row.columns.get(rowFormat.columnIndexes.get("g")).set(0.2);
		row.columns.get(rowFormat.columnIndexes.get("h")).set("foo");
		row.columns.get(rowFormat.columnIndexes.get("i")).set(new byte[] {4, 5, 6, 7});
		row.columns.get(rowFormat.columnIndexes.get("j")).set("bar");
		
		table.printAllRows(TableFormat.FORMAT_CELLS);
	}

}
