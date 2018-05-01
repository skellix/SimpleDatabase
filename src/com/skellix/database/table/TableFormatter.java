package com.skellix.database.table;

public class TableFormatter {
	
	public static void printTableRow(ExperimentalTable table, TableFormat format) {
		
		if (format == TableFormat.FORMAT_CELLS) {
			
			System.out.print("|");
			System.out.printf(getRowFormatString(table, format), (Object[]) table.getColumnLabels());
			System.out.println("|");
		}
	}
	
	public static String getRowFormatString(ExperimentalTable table, TableFormat format) {
		
		StringBuilder sb = new StringBuilder();
		String last = null;
		
		if (format == TableFormat.FORMAT_CELLS) {
			
			sb.append("|");
			
		} else if (format == TableFormat.FORMAT_CSV) {
			
			//
		}
		
		for (String key : table.rowFormat.keySet()) {
			
			if (last != null) {
				
				if (format == TableFormat.FORMAT_CELLS) {
					
					sb.append("|");
					
				} else if (format == TableFormat.FORMAT_CSV) {
					
					sb.append(",");
				}
			}
			last = key;
			sb.append(String.format("%%%ds", table.rowFormat.get(key)));
		}
		
		if (format == TableFormat.FORMAT_CELLS) {
			
			sb.append("|\n");
			
		} else if (format == TableFormat.FORMAT_CSV) {
			
			sb.append("\n");
		}
		
		return sb.toString();
	}
	
	public static void printTableStart(ExperimentalTable table, TableFormat format) {
		
		if (format == TableFormat.FORMAT_CELLS) {
			
			System.out.print(",");
			for (int i = table.rowSize ; i >= 0 ; i --) {
				
				System.out.print("-");
			}
			System.out.println(".");
			System.out.printf(getRowFormatString(table, format), (Object[]) table.getColumnLabels());
			
		} else if (format == TableFormat.FORMAT_CSV) {
			
			System.out.printf(getRowFormatString(table, format), (Object[]) table.getColumnLabels());
		}
	}
	
	public static void printTableEnd(ExperimentalTable table, TableFormat format) {
		
		if (format == TableFormat.FORMAT_CELLS) {
			System.out.print("`");
			for (int i = table.rowSize ; i >= 0 ; i --) {
				
				System.out.print("-");
			}
			System.out.println("'");
			
		} else if (format == TableFormat.FORMAT_CSV) {
			
			//
		}
	}

}
