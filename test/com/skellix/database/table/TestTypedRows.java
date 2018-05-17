package com.skellix.database.table;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import com.skellix.database.row.RowFormat;
import com.skellix.database.row.RowFormatter;
import com.skellix.database.row.RowFormatterException;
import com.skellix.database.row.TableRow;
import com.skellix.database.session.Session;

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
		
		RowFormat rowFormat = null;
		try {
			rowFormat = RowFormatter.parse(sb.toString());
		} catch (RowFormatterException e1) {
			e1.printStackTrace();
		}
		
		ExperimentalTable table = ExperimentalTable.getOrCreate(directory, rowFormat);
		try {
			table.deleteTable();
			table.initTable();
		} catch (IOException e) {
			e.printStackTrace();
		}
		

		try (Session session = Session.createNewSession(true)) {
			
			TableRow row = table.addRow(session);
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
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		table.printAllRows(TableFormat.FORMAT_CELLS);
	}

}
