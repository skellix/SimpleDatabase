package com.skellix.database.table.query.node;

import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;

import com.skellix.database.session.Session;
import com.skellix.database.table.AliasedTable;
import com.skellix.database.table.ExperimentalTable;
import com.skellix.database.table.FilteredTable;
import com.skellix.database.table.RowFormat;
import com.skellix.database.table.TableRow;
import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class AliasTableQueryNode extends QueryNode {

	private QueryNode tableNode;
	private String alias;

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
		
		aliasAs(previousQueryNode, nextNode);
		replaceNode.replaceWith(this);
		
		previousNode.removeFromParent();
		nextNode.removeFromParent();
		
		children.add(previousNode);
		children.add(nextNode);
		
		copyValuesFrom(replaceNode);
		
		resultType = ExperimentalTable.class;
		
		return this;
	}

	private void aliasAs(QueryNode tableNode, TreeNode aliasNode) {
		this.tableNode = tableNode;
		alias = aliasNode.getLabel();
	}

	@Override
	public Object query(Session session) throws Exception {
		
		Object tableQueryResult = tableNode.query(session);
		ExperimentalTable table = (ExperimentalTable) tableQueryResult;
		RowFormat rowFormat = table.rowFormat;
		
		Lock lock = table.getReadLock();
		session.addLock(lock);
		
		try {
			
			lock.lock();
			
			AliasedTable combinedTable = new AliasedTable().filter(rowFormat);
			combinedTable.setSource(table.getName(), table);
			combinedTable.setAlias(alias);
			
			return combinedTable;
		
		} catch (Exception e) {
			
			throw e;
			
		} finally {
			
			lock.unlock();
		}
	}

	@Override
	public String generateCode() {
		
		StringBuilder sb = new StringBuilder();
		
		if (tableNode != null) {
			
			sb.append(tableNode.generateCode());
		}
		
		sb.append(" as ");
		sb.append(alias);
		
		return sb.toString();
	}

}
