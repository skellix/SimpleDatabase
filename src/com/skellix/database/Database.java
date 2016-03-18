package com.skellix.database;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import com.skellix.treeparser.TreeNode;
import com.skellix.treeparser.TreeParser;

public abstract class Database {

	public abstract void init();
	
	public abstract HashMap<String, Table> getTables();
	
	public static HashMap<String, Table> defineDatabase(File databaseLocation, String tableDef) {
		
		HashMap<String, Table> tables = new HashMap<String, Table>();
		
		TreeNode def = TreeParser.parse(tableDef);
		
		for (int i = 0 ; i < def.children.size() ; i ++) {
			
			TreeNode tableNode = def.children.get(i);
			
			if (tableNode.hasChildren()) {
				continue;
			}
			
			String tableName = tableNode.getLabel();
			
			i ++;
			
			TreeNode tableChildrenNode = tableNode.getSiblingNode(i);
			
			if (tableChildrenNode == null) {
				System.err.printf("[ERROR] Expected table row deffinition after table '%s' on line %d\n", tableName, tableNode.line);
				System.exit(-1);
			}
			
			if (!tableChildrenNode.hasChildren()) {
				System.err.printf("[ERROR] Row deffinition for table '%s' has no columns on line %d\n", tableName, tableChildrenNode.line);
				System.exit(-1);
			}
			
			ArrayList<TreeNode> tableChildren = tableChildrenNode.children;
			
			HashMap<String, Integer> tableRowDef = new HashMap<String, Integer>();
			
			TreeNode last = tableChildrenNode;
			for (int j = 0 ; j < tableChildren.size() ; j += 3) {
				
				if (j + 1 >= tableChildren.size()) {
					System.err.printf("[ERROR] Column definition expected for table '%s' after '%s' on line %d\n", tableName, last.getLabel(), last.line);
					System.exit(-1);
				}
				
				TreeNode columnNameNode = tableChildren.get(j);
				
				if (columnNameNode == null) {
					System.err.printf("[ERROR] Column name expected for table '%s' after '%s' on line %d\n", tableName, last.getLabel(), last.line);
					System.exit(-1);
				}
				
				String columnName = columnNameNode.getLabel();
				last = columnNameNode;
				
				TreeNode columnWidthNode = tableChildren.get(j + 1);
				
				if (columnWidthNode == null) {
					System.err.printf("[ERROR] Column width expected for table '%s' after '%s' on line %d\n", tableName, last.getLabel(), last.line);
					System.exit(-1);
				}
				
				Integer columnWidth = Integer.parseInt(columnWidthNode.getLabel());
				
				last = columnWidthNode;
				tableRowDef.put(columnName, columnWidth);
				
				if (j + 2 < tableChildren.size()) {
				
					TreeNode commaNode = tableChildren.get(j + 2);
					
					if (commaNode != null && !commaNode.getLabel().equals(",")) {
						
						System.err.printf("[ERROR] Comma expected in table '%s' but found '%s' on line %d\n", tableName, commaNode.getLabel(), commaNode.line);
						System.exit(-1);
					}
				}
				
			}
			
			/* add the table to the database */
			tables.put(tableName, new Table(new File(databaseLocation, tableName), tableRowDef));
		}
		
		return tables;
	}
}
