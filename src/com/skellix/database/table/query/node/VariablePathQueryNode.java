package com.skellix.database.table.query.node;

import java.util.Map;

import com.skellix.database.session.Session;
import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class VariablePathQueryNode extends QueryNode {
	
	public VariablePathQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		int index = replaceNode.parent.children.indexOf(replaceNode);
		String label = replaceNode.getLabel();
		
		TreeNode previousNode = replaceNode.getPreviousSibling();
		
		if (previousNode == null) {
			
			String errorString = String.format("ERROR: expected table name before '%s' at %d, %d"
					, label, replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		TreeNode nextNode = replaceNode.getNextSibling();
		
		if (nextNode == null) {
			
			String errorString = String.format("ERROR: expected column name after '%s' at %d, %d"
					, label, replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		replaceNode.parent.children.remove(replaceNode);
		VariablePathQueryNode queryNode = new VariablePathQueryNode().pathOf(previousNode, nextNode);
		replaceNode.parent.children.add(index, queryNode);
		
		previousNode.parent.children.remove(previousNode);
		nextNode.parent.children.remove(nextNode);
		
		queryNode.copyValuesFrom(replaceNode);
		
		return queryNode;
	}

	public VariablePathQueryNode pathOf(TreeNode previousNode, TreeNode nextNode) {
		
		this.children.clear();
		this.children.add(previousNode);
		this.children.add(nextNode);
		return this;
	}

	@Override
	public Object query(Session session) throws Exception {
		
		TreeNode tableIdNode = children.get(0);
		TreeNode columnNameNode = children.get(1);
		
		String tableId = tableIdNode.getLabel();
		String columnName = columnNameNode.getLabel();
		
		Object tableObj = session.variables.get(tableId);
		
		if (tableObj instanceof Map) {
			
			Map<String, Object> map = (Map<String, Object>) tableObj;
			
			if (!map.containsKey(columnName)) {
				
				throw new Exception("Table '" + tableId + "' does not contain the column '" + columnName + "'");
			}
			
			Object result = map.get(columnName);
			return result;
			
		} else {
			
			throw new Exception("Expected table name but found '" + tableId + "'");
		}
		
//		if (tableObj instanceof ExperimentalTable) {
//			
//			ExperimentalTable table = (ExperimentalTable) tableObj;
//			
//			RowFormat rowFormat = table.rowFormat;
//			
//			if (!rowFormat.columnNames.contains(columnName)) {
//				
//				throw new Exception("Table '" + tableId + "' does not contain the column '" + columnName + "'");
//			}
//			
//			//
//			
//		} else {
//			
//			throw new Exception("Expected table name but found '" + tableId + "'");
//		}
//		
//		return null;
	}

	@Override
	public String generateCode() {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append(children.get(0).getLabel()); 
		sb.append(" . ");
		sb.append(children.get(1).getLabel());
		
		return sb.toString();
	}
	
	@Override
	public String toString() {
		
		return generateCode();
	}

}
