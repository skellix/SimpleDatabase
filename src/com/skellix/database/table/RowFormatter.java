package com.skellix.database.table;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import treeparser.TreeNode;
import treeparser.TreeParser;

/***
 * Using the {@link #parse(String)} function, a string representing the row format
 * </br>of the table can be converted into a RowFormat object for creating a table.
 * </br>The format used is {column_type column_name column_size}
 * </br>The valid column types are:
 * <ul>
 *   <li>boolean</li>
 *   <li>byte</li>
 *   <li>char</li>
 *   <li>int</li>
 *   <li>long</li>
 *   <li>float</li>
 *   <li>double</li>
 *   <li>string</li>
 *   <li>byte_array</li>
 *   <li>object</li>
 * </ul>
 * The column_size argument is only required for the types:
 * <ul>
 *   <li>string</li>
 *   <li>byte_array</li>
 *   <li>object</li>
 * </ul>
 */
public class RowFormatter {

	public static RowFormat parse(String formatString) {
		
		TreeNode parsed = TreeParser.parse(formatString);
		
		List<String> columnNames = new ArrayList<>();
		Map<String, ColumnType> columnTypes = new LinkedHashMap<>();
		Map<String, Integer> columnSizes = new LinkedHashMap<>();
		
		for (TreeNode parsedChild : parsed.children) {
			
			if (!parsedChild.hasChildren()) {
				
				System.err.printf("ERROR: expected a group at %d:%d\n"
						, parsedChild.line, parsedChild.getStartColumn());
				System.exit(-1);
			}
			
			if (parsedChild.children.size() < 2 || parsedChild.children.size() > 3) {
				
				System.err.printf("ERROR: expected column in the format {type label} or {type label byte_size} at %d:%d\n"
						, parsedChild.line, parsedChild.getStartColumn());
				System.exit(-1);
			}
			
			TreeNode typeNode = parsedChild.children.get(0);
			TreeNode nameNode = parsedChild.children.get(1);
			
			String name = nameNode.getLabel();
			columnNames.add(name);
			
			String typeString = typeNode.getLabel();
			ColumnType type = ColumnType.parse(typeString);
			columnTypes.put(name, type);
			
			if (parsedChild.children.size() == 3) {
			
				TreeNode lengthNode = parsedChild.children.get(2);
				String lengthString = lengthNode.getLabel();
				try {
					
					columnSizes.put(name, Integer.parseInt(lengthString));
					
				} catch (NumberFormatException e) {
					
					System.err.printf("ERROR: invalid number '%s' for column byte_size at %d:%d\n"
							, lengthString, parsedChild.line, parsedChild.getStartColumn());
					System.exit(-1);
				}
			} else {
				
				switch (type) {
				case STRING:
				case BYTE_ARRAY:
				case OBJECT:
					System.err.printf(
							"ERROR: STRING, BYTE_ARRAY, and OBJECT types must specify a byte length in th format {type label byte_size} at %d:%d\n"
							,parsedChild.line, parsedChild.getStartColumn());
					System.exit(-1);
				default:
					columnSizes.put(name, type.defaultByteLength());
				}
			}
			
			
		}
		
		return new RowFormat(columnNames, columnTypes, columnSizes);
	}
}
