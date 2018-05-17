package com.skellix.database.table.query.node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.Lock;

import com.skellix.database.row.RowFormat;
import com.skellix.database.session.Session;
import com.skellix.database.table.Table;
import com.skellix.database.table.ReformattedTable;
import com.skellix.database.table.query.exception.QueryParseException;
import com.skellix.database.table.query.type.ListType;
import com.skellix.database.table.query.type.ValueType;

import treeparser.TreeNode;

public class SelectQueryNode extends QueryNode {

	private QueryNode tableNode;
	private QueryNode selectNode;

	@Override
	public QueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		TreeNode previousNode = replaceNode.getPreviousSibling();
		
		if (previousNode == null || !(previousNode instanceof QueryNode)) {
			
			String errorString = String.format("ERROR: expected table before '%s' at %d, %d"
					, replaceNode.getLabel(), replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		QueryNode previousQueryNode = (QueryNode) previousNode;
		
		if (!Table.class.isAssignableFrom(previousQueryNode.resultType)) {
			
			String errorString = String.format("ERROR: expected table before '%s' at %d, %d"
					, replaceNode.getLabel(), previousQueryNode.line, previousQueryNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		TreeNode nextNode = replaceNode.getNextSibling();
		
		if (nextNode == null) {
			
			String errorString = String.format("ERROR: expected list or column name after '%s' at %d, %d"
					, replaceNode.getLabel(), replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		previousNode.removeFromParent();
		nextNode.removeFromParent();
		
		if (!(nextNode instanceof QueryNode)) {
			
			nextNode = new ValueQueryNode().parse(nextNode);
		}
		
		QueryNode nextQueryNode = (QueryNode) nextNode;
		
		if (!(ListType.class.isAssignableFrom(nextQueryNode.resultType) || ValueType.class.isAssignableFrom(nextQueryNode.resultType))) {
			
			String errorString = String.format("ERROR: expected list after '%s' at %d, %d"
					, replaceNode.getLabel(), nextQueryNode.line, nextQueryNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		select(previousQueryNode, nextQueryNode);
		replaceNode.replaceWith(this);
		copyValuesFrom(replaceNode);
		parent = replaceNode.parent;
		
		children.add(previousNode);
		children.add(nextNode);
		
		resultType = Table.class;
		
		return this;
	}

	private void select(QueryNode tableNode, QueryNode selectNode) {
		
		this.tableNode = tableNode;
		this.selectNode = selectNode;
	}

	@Override
	public Object query(Session session) throws Exception {
		
		Object tableQueryResult = tableNode.query(session);
		
		if (tableQueryResult instanceof Table) {
			
			Table table = (Table) tableQueryResult;
			RowFormat rowFormat = table.rowFormat;
			
			Lock lock = table.getReadLock();
			session.addLock(lock);
			
			try {
				
				lock.lock();
				
				List<Object> columnList = new ArrayList<>();
				
				Object select = (List<Object>) selectNode.query(session);
				
				if (select instanceof Collection) {
					
					Collection collection = (Collection) select;
					columnList.addAll(collection);
					
				} else {
					
					columnList.add(select);
				}
				
				ReformattedTable combinedTable = new ReformattedTable().createFormat(rowFormat, columnList);
				combinedTable.setSource(table.getName(), table);
				combinedTable.setSession(session);
				
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
