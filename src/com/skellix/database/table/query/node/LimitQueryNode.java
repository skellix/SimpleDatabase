package com.skellix.database.table.query.node;

import java.util.concurrent.locks.Lock;

import com.skellix.database.row.RowFormat;
import com.skellix.database.session.Session;
import com.skellix.database.table.ExperimentalTable;
import com.skellix.database.table.LimitedTable;
import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class LimitQueryNode extends QueryNode {

	private QueryNode tableNode;
	private long limit;

	@Override
	public QueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		TreeNode previousNode = replaceNode.getPreviousSibling();
		
		if (previousNode == null || !(previousNode instanceof QueryNode)) {
			
			String errorString = String.format("ERROR: expected table before '%s' at %d, %d"
					, replaceNode.getLabel(), replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		QueryNode previousQueryNode = (QueryNode) previousNode;
		
		if (!ExperimentalTable.class.isAssignableFrom(previousQueryNode.resultType)) {
			
			String errorString = String.format("ERROR: expected table before '%s' at %d, %d"
					, replaceNode.getLabel(), previousQueryNode.line, previousQueryNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		TreeNode nextNode = replaceNode.getNextSibling();
		
		if (nextNode == null) {
			
			String errorString = String.format("ERROR: expected number or variable after '%s' at %d, %d"
					, replaceNode.getLabel(), replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		long limit = 0L;
		try {
		
			limit = Long.parseLong(nextNode.getLabel());
			
		} catch (NumberFormatException e) {
			
			String errorString = String.format("ERROR: expected number or variable after '%s' at %d, %d"
					, replaceNode.getLabel(), nextNode.line, nextNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		setLimit(previousQueryNode, limit);
		replaceNode.replaceWith(this);
		
		previousNode.removeFromParent();
		nextNode.removeFromParent();
		
		children.add(previousQueryNode);
		children.add(nextNode);
		
		copyValuesFrom(replaceNode);
		
		resultType = ExperimentalTable.class;
		
		return this;
	}

	private void setLimit(QueryNode tableNode, long limit) {
		this.tableNode = tableNode;
		this.limit = limit;
	}

	@Override
	public Object query(Session session) throws Exception {
		
		Object tableQueryResult = tableNode.query(session);
		
		if (tableQueryResult instanceof ExperimentalTable) {
			
			ExperimentalTable table = (ExperimentalTable) tableQueryResult;
			RowFormat rowFormat = table.rowFormat;
			
			Lock lock = table.getReadLock();
			session.addLock(lock);
			
			try {
				
				lock.lock();
				
				LimitedTable combinedTable = new LimitedTable().limit(rowFormat, limit);
				combinedTable.setSource(table.getName(), table);
				
				return combinedTable;
			
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
		// TODO Auto-generated method stub
		return null;
	}

}
