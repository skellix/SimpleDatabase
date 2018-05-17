package com.skellix.database.table;

import java.util.stream.Collectors;

public class TableFormatter {
	
	public static void printTableRow(ExperimentalTable table, TableRow row, TableFormat format) {
		
		System.out.printf(getRowFormatString(table, format), row.getValueArray());
		System.out.println();
	}
	
	public static String getRowFormatString(ExperimentalTable table, TableFormat format) {
		
		StringBuilder sb = new StringBuilder();
		String last = null;
		
		if (format == TableFormat.FORMAT_CELLS) {
			
			sb.append("|");
			
		} else if (format == TableFormat.FORMAT_CSV) {
			
			//
		}
		
		for (String key : table.rowFormat.columnNames) {
			
			if (last != null) {
				
				if (format == TableFormat.FORMAT_CELLS) {
					
					sb.append("|");
					
				} else if (format == TableFormat.FORMAT_CSV) {
					
					sb.append(",");
				}
			}
			last = key;
			Integer byteSize = table.rowFormat.columnSizes.get(key);
			String formatString = table.rowFormat.columnTypes.get(key).formatString(byteSize);
			sb.append(formatString);
		}
		
		if (format == TableFormat.FORMAT_CELLS) {
			
			sb.append("|");
			
		} else if (format == TableFormat.FORMAT_CSV) {

			//
		}
		
		return sb.toString();
	}
	
	public static void printTableStart(ExperimentalTable table, TableFormat format) {
		
		printTableStart(table.rowFormat, format);
	}
	
	public static void printTableStart(RowFormat rowFormat, TableFormat format) {
		
		System.out.print(getTableStart(rowFormat, format));
	}
	
	public static String getTableStart(RowFormat rowFormat, TableFormat format) {
		
		StringBuilder result = new StringBuilder();
		
		if (format == TableFormat.FORMAT_CELLS) {
			
			StringBuilder sb = new StringBuilder();
			sb.append(',');
			String last = null;
			for (String label : rowFormat.columnNames) {
				
				
				if (last != null) {
					
					sb.append('-');
					
				}
				last = label;
				Integer byteSize = rowFormat.columnSizes.get(label);
				int stringLength = rowFormat.columnTypes.get(label).stringLength(byteSize);
				
				for (int j = 0 ; j < stringLength ; j ++) {
					
					sb.append('-');
				}
			}
			sb.append('.');
			result.append(sb.toString());
			result.append('\n');
			
			sb.setLength(0);
			last = null;
			sb.append("|");
			for (String label : rowFormat.columnNames) {
				
				if (last != null) {
					
					sb.append("|");
				}
				last = label;
				Integer byteSize = rowFormat.columnSizes.get(label);
				int stringLength = rowFormat.columnTypes.get(label).stringLength(byteSize);
				sb.append(String.format(String.format("%%%ds", stringLength), label));
			}
			sb.append("|");
			result.append(sb.toString());
			result.append('\n');
			
			sb.setLength(0);
			sb.append("|");
			last = null;
			for (String label : rowFormat.columnNames) {
				
				
				if (last != null) {
					
					sb.append('+');
					
				}
				last = label;
				Integer byteSize = rowFormat.columnSizes.get(label);
				int stringLength = rowFormat.columnTypes.get(label).stringLength(byteSize);
				
				for (int j = 0 ; j < stringLength ; j ++) {
					
					sb.append('-');
				}
			}
			sb.append("|");
			result.append(sb.toString());
			result.append('\n');
			
		} else if (format == TableFormat.FORMAT_CSV) {
			
			StringBuilder sb = new StringBuilder();
			String last = null;
			for (String label : rowFormat.columnNames) {
				
				if (last != null) {
					
					sb.append(",");
				}
				last = label;
				sb.append(String.format("%s ", label));
			}
			result.append(sb.toString());
			result.append('\n');
		}
		
		return result.toString();
	}
	
	public static void printTableEnd(ExperimentalTable table, TableFormat format) {
		
		printTableEnd(table.rowFormat, format);
	}
	
	public static void printTableEnd(RowFormat rowFormat, TableFormat format) {
		
		System.out.print(getTableEnd(rowFormat, format));
	}
	
	public static String getTableEnd(RowFormat rowFormat, TableFormat format) {
		
		if (format == TableFormat.FORMAT_CELLS) {
			
			StringBuilder sb = new StringBuilder();
			sb.append('`');
			String last = null;
			for (String label : rowFormat.columnNames) {
				
				
				if (last != null) {
					
					sb.append('-');
					
				}
				last = label;
				Integer byteSize = rowFormat.columnSizes.get(label);
				int stringLength = rowFormat.columnTypes.get(label).stringLength(byteSize);
				
				for (int j = 0 ; j < stringLength ; j ++) {
					
					sb.append('-');
				}
			}
			sb.append('\'');
			sb.append('\n');
			return sb.toString();
			
		} else if (format == TableFormat.FORMAT_CSV) {
			
			return "";
		}
		
		return "";
	}

	public static void printRows(ExperimentalTable table) {
		
		new Runnable() {
			
			boolean queryHeaderPrinted = false;
			RowFormat resultRowFormat = null;
			int rowCount = 0;
			
			@Override
			public void run() {
				
				table.stream().forEach(row -> {

					RowFormat rowFormat = row.rowFormat;
					
					String rowString = rowFormat.columnNames.stream().map(columnName -> {
						
						int byteSize = rowFormat.columnSizes.get(columnName);
						String formatString = rowFormat.columnTypes.get(columnName).formatString(byteSize);
						Object value = row.columns.get(rowFormat.columnIndexes.get(columnName)).get();
						
						if (!queryHeaderPrinted) {
							
							resultRowFormat = rowFormat;
							System.out.print(rowFormat.getHeader(TableFormat.FORMAT_CELLS));
							queryHeaderPrinted = true;
						}
						
						return String.format(formatString, value);
					}).collect(Collectors.joining("|"));
					
					System.out.printf("|%s|\n", rowString);
					rowCount ++;
				});
				
				if (resultRowFormat != null) {
				
					System.out.print(resultRowFormat.getEnd(TableFormat.FORMAT_CELLS));
				}
			}
		}.run();
	}

}
