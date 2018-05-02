package com.skellix.database.table;

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
		
		if (format == TableFormat.FORMAT_CELLS) {
			
			StringBuilder sb = new StringBuilder();
			sb.append(',');
			String last = null;
			for (String label : table.rowFormat.columnNames) {
				
				
				if (last != null) {
					
					sb.append('-');
					
				}
				last = label;
				Integer byteSize = table.rowFormat.columnSizes.get(label);
				int stringLength = table.rowFormat.columnTypes.get(label).stringLength(byteSize);
				
				for (int j = 0 ; j < stringLength ; j ++) {
					
					sb.append('-');
				}
			}
			sb.append('.');
			System.out.println(sb.toString());
			
			sb.setLength(0);
			last = null;
			sb.append("|");
			for (String label : table.rowFormat.columnNames) {
				
				if (last != null) {
					
					sb.append("|");
				}
				last = label;
				Integer byteSize = table.rowFormat.columnSizes.get(label);
				int stringLength = table.rowFormat.columnTypes.get(label).stringLength(byteSize);
				sb.append(String.format(String.format("%%%ds", stringLength), label));
			}
			sb.append("|");
			System.out.println(sb.toString());
			
			sb.setLength(0);
			sb.append("|");
			last = null;
			for (String label : table.rowFormat.columnNames) {
				
				
				if (last != null) {
					
					sb.append('+');
					
				}
				last = label;
				Integer byteSize = table.rowFormat.columnSizes.get(label);
				int stringLength = table.rowFormat.columnTypes.get(label).stringLength(byteSize);
				
				for (int j = 0 ; j < stringLength ; j ++) {
					
					sb.append('-');
				}
			}
			sb.append("|");
			System.out.println(sb.toString());
			
		} else if (format == TableFormat.FORMAT_CSV) {
			
			StringBuilder sb = new StringBuilder();
			String last = null;
			for (String label : table.rowFormat.columnNames) {
				
				if (last != null) {
					
					sb.append(",");
				}
				last = label;
				sb.append(String.format("%s ", label));
			}
			System.out.println(sb.toString());
		}
	}
	
	public static void printTableEnd(ExperimentalTable table, TableFormat format) {
		
		if (format == TableFormat.FORMAT_CELLS) {
			
			StringBuilder sb = new StringBuilder();
			sb.append('`');
			String last = null;
			for (String label : table.rowFormat.columnNames) {
				
				
				if (last != null) {
					
					sb.append('-');
					
				}
				last = label;
				Integer byteSize = table.rowFormat.columnSizes.get(label);
				int stringLength = table.rowFormat.columnTypes.get(label).stringLength(byteSize);
				
				for (int j = 0 ; j < stringLength ; j ++) {
					
					sb.append('-');
				}
			}
			sb.append('\'');
			System.out.println(sb.toString());
			
		} else if (format == TableFormat.FORMAT_CSV) {
			
			//
		}
	}

}
