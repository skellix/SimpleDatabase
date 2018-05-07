package com.skellix.database.table.query;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.skellix.database.session.Session;
import com.skellix.database.table.ExperimentalTable;
import com.skellix.database.table.RowFormat;
import com.skellix.database.table.RowFormatter;
import com.skellix.database.table.RowFormatterException;
import com.skellix.database.table.TableFormat;
import com.skellix.database.table.TableFormatter;
import com.skellix.database.table.TableRow;

class TestNodeGetQuery {

	@Test
	void test() {
		
		Path directory = Paths.get("testData", "experimental");
		
		StringBuilder sb = new StringBuilder();
		sb.append("{string name 32}");
		sb.append("{long age}");
		
		RowFormat rowFormat = null;
		try {
			rowFormat = RowFormatter.parse(sb.toString());
		} catch (RowFormatterException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		
		Integer name = rowFormat.columnIndexes.get("name");
		Integer age = rowFormat.columnIndexes.get("age");
		
		ExperimentalTable table = ExperimentalTable.getOrCreate(directory, rowFormat);
		try {
			table.deleteTable();
		} catch (IOException e) {
			e.printStackTrace();
			fail("Unable to delete table");
		}
		table.initTable();
		
		{
			TableRow row = table.addRow();
			row.columns.get(name).set("test");
			row.columns.get(age).set(50L);
		}
		
		QueryNode addRowQueryNode = null;
		try {
			String queryString = "table 'testData/experimental' getRows";
			addRowQueryNode = QueryNodeParser.parse(queryString);
		} catch (QueryParseException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		
		try (Session session = Session.createNewSession(true)) {
			System.out.println("Starting query");
			Object result = session.query(addRowQueryNode);
			
			if (result instanceof Stream) {
				Stream<TableRow> stream = (Stream<TableRow>) result;
				
				TableFormatter.printTableStart(table, TableFormat.FORMAT_CELLS);
				stream.forEach(row -> TableFormatter.printTableRow(table, row, TableFormat.FORMAT_CELLS));
				TableFormatter.printTableEnd(table, TableFormat.FORMAT_CELLS);
			}
			
			System.out.println("Query complete");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
