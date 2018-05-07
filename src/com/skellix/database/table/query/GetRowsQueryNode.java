package com.skellix.database.table.query;

import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;

import com.skellix.database.session.Session;
import com.skellix.database.table.ExperimentalTable;
import com.skellix.database.table.RowFormat;
import com.skellix.database.table.TableRow;

import treeparser.TreeNode;

public class GetRowsQueryNode extends QueryNode {
	
	private TableQueryNode tableQuery;

	public static GetRowsQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		int index = replaceNode.parent.children.indexOf(replaceNode);
		String label = replaceNode.getLabel();

		TreeNode previousNode = replaceNode.getPreviousSibling();
		
		if (previousNode == null || !(previousNode instanceof TableQueryNode)) {
			
			String errorString = String.format("ERROR: expected table before '%s' at %d, %d"
					, label, replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		replaceNode.parent.children.remove(replaceNode);
		GetRowsQueryNode queryNode = new GetRowsQueryNode().streamRowsFrom((TableQueryNode) previousNode);
		replaceNode.parent.children.add(index, queryNode);
		
		previousNode.parent.children.remove(previousNode);
		
		queryNode.copyValuesFrom(replaceNode);
		
		return queryNode;
	}

	private GetRowsQueryNode streamRowsFrom(TableQueryNode tableQuery) {
		
		this.tableQuery = tableQuery;
		return this;
	}

	@Override
	public Object query(Session session) throws Exception {
		
		Object tableQueryResult = tableQuery.query(session);
		
		if (tableQueryResult instanceof ExperimentalTable) {
			
			ExperimentalTable table = (ExperimentalTable) tableQueryResult;
			RowFormat rowFormat = table.rowFormat;
			
			Lock lock = table.getReadLock();
			session.addLock(lock);
			
			try {
				
				lock.lock();
				
				Stream<TableRow> rows = table.stream();
				return rows;
			
			} catch (Exception e) {
				
				throw e;
				
			} finally {
				
				lock.unlock();
			}
		}
		
		return null;
	}

	@Override
	public String generateCode() {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append(tableQuery.generateCode());
		sb.append(" getRows");
		
		return sb.toString();
	}

}
