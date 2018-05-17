package com.skellix.database.table;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

	public static List<List<TreeNode>> getFormatParts(String formatString) throws RowFormatterException {
		
		TreeNode parsed = TreeParser.parse(formatString);
		
		List<TreeNode> types = new ArrayList<>();
		List<TreeNode> names = new ArrayList<>();
		List<TreeNode> sizes = new ArrayList<>();
		
		Set<String> columnNamesSet = new LinkedHashSet<>();
		
		for (TreeNode parsedChild : parsed.children) {
			
			if (!parsedChild.hasChildren()) {
				
				String message = String.format("ERROR: expected a group at %d:%d\n"
						, parsedChild.line, parsedChild.getStartColumn());
				throw new RowFormatterException(message);
			}
			
			if (parsedChild.children.size() < 2 || parsedChild.children.size() > 3) {
				
				String message = String.format("ERROR: expected column in the format {type label} or {type label byte_size} at %d:%d\n"
						, parsedChild.line, parsedChild.getStartColumn());
				throw new RowFormatterException(message);
			}
			
			TreeNode typeNode = parsedChild.children.get(0);
			types.add(typeNode);
			
			if (typeNode.hasChildren()) {
				
				String message = String.format("ERROR: expected column type in the format {type label} or {type label byte_size} at %d:%d\n"
						, typeNode.line, typeNode.getStartColumn());
				throw new RowFormatterException(message);
			}
			
			String typeString = typeNode.getLabel();
			ColumnType type = ColumnType.parse(typeString);
			
			TreeNode nameNode = parsedChild.children.get(1);
			names.add(nameNode);
			
			if (nameNode.hasChildren()) {
				
				String message = String.format("ERROR: expected column label in the format {type label} or {type label byte_size} at %d:%d\n"
						, nameNode.line, nameNode.getStartColumn());
				throw new RowFormatterException(message);
			}
			
			String columnName = nameNode.getLabel();
			
			if (columnNamesSet.contains(columnName)) {
				
				String message = String.format("ERROR: found duplicate column label '%s' at %d:%d\n"
						, columnName, parsedChild.line, parsedChild.getStartColumn());
				throw new RowFormatterException(message);
			}
			
			columnNamesSet.add(columnName);
			
			if (parsedChild.children.size() == 3) {
			
				TreeNode lengthNode = parsedChild.children.get(2);
				String lengthString = lengthNode.getLabel();
				try {
					
					int length = Integer.parseInt(lengthString);
					sizes.add(lengthNode);
					
					if (length < type.defaultByteLength()) {
						
						String message = String.format("ERROR: column type %s requires at least %d bytes at %d:%d\n"
								, type.name(), type.defaultByteLength(), lengthNode.line, lengthNode.getStartColumn());
						throw new RowFormatterException(message);
					}
					
				} catch (NumberFormatException e) {
					
					String message = String.format("ERROR: invalid number '%s' for column byte_size at %d:%d\n"
							, lengthString, parsedChild.line, parsedChild.getStartColumn());
					throw new RowFormatterException(message);
				}
			}
		}
		
		return Arrays.asList(types, names, sizes);
	}
	
	public static RowFormat parse(String formatString) throws RowFormatterException {
		
		TreeNode parsed = TreeParser.parse(formatString);
		
		List<String> columnNames = new ArrayList<>();
		Map<String, ColumnType> columnTypes = new LinkedHashMap<>();
		Map<String, Integer> columnSizes = new LinkedHashMap<>();
		
		Set<String> columnNamesSet = new LinkedHashSet<>();
		
		for (TreeNode parsedChild : parsed.children) {
			
			if (!parsedChild.hasChildren()) {
				
				String message = String.format("ERROR: expected a group at %d:%d\n"
						, parsedChild.line, parsedChild.getStartColumn());
				throw new RowFormatterException(message);
			}
			
			if (parsedChild.children.size() < 2 || parsedChild.children.size() > 3) {
				
				String message = String.format("ERROR: expected column in the format {type label} or {type label byte_size} at %d:%d\n"
						, parsedChild.line, parsedChild.getStartColumn());
				throw new RowFormatterException(message);
			}
			
			TreeNode typeNode = parsedChild.children.get(0);
			
			if (typeNode.hasChildren()) {
				
				String message = String.format("ERROR: expected column type in the format {type label} or {type label byte_size} at %d:%d\n"
						, typeNode.line, typeNode.getStartColumn());
				throw new RowFormatterException(message);
			}
			
			TreeNode nameNode = parsedChild.children.get(1);
			
			if (nameNode.hasChildren()) {
				
				String message = String.format("ERROR: expected column label in the format {type label} or {type label byte_size} at %d:%d\n"
						, parsedChild.line, parsedChild.getStartColumn());
				throw new RowFormatterException(message);
			}
			
			String columnName = nameNode.getLabel();
			columnNames.add(columnName);
			
			if (columnNamesSet.contains(columnName)) {
				
				String message = String.format("ERROR: found duplicate column label '%s' at %d:%d\n"
						, columnName, parsedChild.line, parsedChild.getStartColumn());
				throw new RowFormatterException(message);
			}
			
			columnNamesSet.add(columnName);
			
			String typeString = typeNode.getLabel();
			ColumnType type = ColumnType.parse(typeString);
			columnTypes.put(columnName, type);
			
			if (parsedChild.children.size() == 3) {
			
				TreeNode lengthNode = parsedChild.children.get(2);
				String lengthString = lengthNode.getLabel();
				try {
					
					int length = Integer.parseInt(lengthString);
					
					if (length < type.minimumByteLength()) {
						
						String message = String.format("ERROR: column type %s requires at least %d bytes at %d:%d\n"
								, type.name(), type.minimumByteLength(), lengthNode.line, lengthNode.getStartColumn());
						throw new RowFormatterException(message);
					}
					
					columnSizes.put(columnName, length);
					
				} catch (NumberFormatException e) {
					
					String message = String.format("ERROR: invalid number '%s' for column byte_size at %d:%d\n"
							, lengthString, parsedChild.line, parsedChild.getStartColumn());
					throw new RowFormatterException(message);
				}
			} else {
				
				switch (type) {
				case STRING:
				case BYTE_ARRAY:
				case OBJECT:
					String message = String.format(
							"ERROR: STRING, BYTE_ARRAY, and OBJECT types must specify a byte length in th format {type label byte_size} at %d:%d\n"
							,parsedChild.line, parsedChild.getStartColumn());
					throw new RowFormatterException(message);
				default:
					columnSizes.put(columnName, type.defaultByteLength());
				}
			}
			
			
		}
		
		return new RowFormat(columnNames, columnTypes, columnSizes);
	}

	public static RowFormat parse(Path formatPath) throws IOException,RowFormatterException {
		
		String formatString = Files.readAllLines(formatPath).stream().collect(Collectors.joining(" "));
		return parse(formatString);
	}
}
