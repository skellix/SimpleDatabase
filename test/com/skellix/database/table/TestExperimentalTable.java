package com.skellix.database.table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class TestExperimentalTable {

	@Test
	void test() {
		
		Path directory = Paths.get("testData", "experimental");
		
		if (Files.exists(directory)) {
			
			try {
				Files.walk(directory).forEach(path -> {
					
					System.out.println("deleting: " + path.toAbsolutePath().toString());
					
					if (!Files.isDirectory(path)) {
						
						try {
							Files.delete(path);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				});
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		
		Map<String, Integer> tableFormat = new LinkedHashMap<>();
		tableFormat.put("username", 16);
		tableFormat.put("password", 32);
		
		ExperimentalTable table = new ExperimentalTable(directory, tableFormat);
		
		System.out.println("limit: " + table.buffer.limit());
		
		Integer USERNAME = table.columnOffset.get("username");
		Integer PASSWORD = table.columnOffset.get("password");
		
		// 0       in    0.015s
		// 10      in    0.021s
		// 100     in    0.055s
		// 1000    in    0.295s
		// 10000   in    2.631s
		// 100000  in   25.587s
		// 1000000 in  264.326s
		
		for (int i = 0 ; i < 1000000 ; i ++) {
			
			TableRow row = table.addRow();
			row.setString(USERNAME, "test");
			row.setString(PASSWORD, "testpassword");
		}
		
		System.out.println("limit: " + table.buffer.limit());
		System.out.println("row count: " + table.rowCount());
		
		System.out.println("running query");
		
		// 1       in    0.020s
		// 10      in    0.024s
		// 100     in    0.052s
		// 1000    in    0.310s
		// 10000   in    2.662s
		// 100000  in   26.541s
		// 1000000 in  263.457s
		
		long queryStart = System.currentTimeMillis();
		
		table.stream().forEach(row -> {
			
			System.out.println(row.getString(USERNAME));
		});
		
		long queryTime = System.currentTimeMillis() - queryStart;
		System.out.println("query time: " + queryTime + "ms");
		
//		System.out.println("count: " + table.stream().count());
	}

}
