package com.skellix.database.table.query;

import java.util.List;
import java.util.Map.Entry;

import com.skellix.database.table.ExperimentalTable;
import com.skellix.database.table.RowFormat;
import com.skellix.database.table.TableRow;

import treeparser.TreeNode;

public class AddRowQueryNode extends QueryNode {
	
	private QueryNode rowQuery;
	private QueryNode destQuery;
	
	public static AddRowQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		int index = replaceNode.parent.children.indexOf(replaceNode);
		String label = replaceNode.getLabel();

		TreeNode previousNode = replaceNode.getPreviousSibling();
		
		if (previousNode == null || !(previousNode instanceof TableQueryNode)) {
			
			String errorString = String.format("ERROR: expected table before '%s' at %d, %d"
					, label, replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		TreeNode nextNode = replaceNode.getNextSibling();
		
		if (nextNode == null || !(nextNode instanceof MapQueryNode)) {
			
			String errorString = String.format("ERROR: expected map after '%s' at %d, %d"
					, label, replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		replaceNode.parent.children.remove(replaceNode);
		AddRowQueryNode queryNode = new AddRowQueryNode().addResultOf((MapQueryNode) nextNode, (TableQueryNode) previousNode);
		replaceNode.parent.children.add(index, queryNode);
		
		previousNode.parent.children.remove(previousNode);
		nextNode.parent.children.remove(nextNode);
		
		queryNode.copyValuesFrom(replaceNode);
		
		return queryNode;
	}

	public AddRowQueryNode addResultOf(QueryNode rowQuery, QueryNode destQuery) {
		
		this.rowQuery = rowQuery;
		this.destQuery = destQuery;
		return this;
	}
	
	@Override
	public Object query() throws Exception {
		
		Object mappings = rowQuery.query();
		
		if (mappings instanceof List) {
			
			List<EntryQueryNode> rowMappings = (List<EntryQueryNode>) mappings;
			Object tableQueryResult = destQuery.query();
			
			if (tableQueryResult instanceof ExperimentalTable) {
				
				ExperimentalTable table = (ExperimentalTable) tableQueryResult;
				RowFormat rowFormat = table.rowFormat;
				
				TableRow row = table.addRow();
				
				for (EntryQueryNode entryNode : rowMappings) {
					
					Object entryObj = entryNode.query();
					
					if (entryObj instanceof Entry) {
						
						Entry<StringQueryNode, QueryNode> entry = (Entry<StringQueryNode, QueryNode>) entryObj;
						String key = (String) entry.getKey().query();
						Object value = entry.getValue().query();
						Object casted = rowFormat.columnTypes.get(key).cast(value);
						row.columns.get(rowFormat.columnIndexes.get(key)).set(casted);
					}
				}
			}
		}
		
		return null;
	}

	@Override
	public String generateCode() {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append(destQuery.generateCode());
		sb.append(" addRow ");
		sb.append(rowQuery.generateCode());
		
		return sb.toString();
	}

}
