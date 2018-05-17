package com.skellix.database.table.query.node;

import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;

import com.skellix.database.session.Session;
import com.skellix.database.table.ExperimentalTable;
import com.skellix.database.table.FilteredTable;
import com.skellix.database.table.RowFormat;
import com.skellix.database.table.TableRow;
import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class WhereQueryNode extends QueryNode {
	
	private QueryNode tableNode;
	private QueryNode joinedTable;
	private QueryNode clause;

	public WhereQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		TreeNode previousNode = replaceNode.getPreviousSibling();
		
		if (previousNode == null) {
			
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
		
		TreeNode clauseNode = replaceNode.getNextSibling();
		
		if (clauseNode == null) {
			
			String errorString = String.format("ERROR: expected table after '%s' at %d, %d"
					, replaceNode.getLabel(), replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		
		useClause((QueryNode) previousNode, (QueryNode) clauseNode);
		replaceNode.replaceWith(this);
		copyValuesFrom(replaceNode);
		parent = replaceNode.parent;
		
		previousNode.removeFromParent();
		clauseNode.removeFromParent();
		
		resultType = ExperimentalTable.class;
		
		return this;
	}

	private WhereQueryNode useClause(QueryNode tableNode, QueryNode clause) {
		
		this.tableNode = tableNode;
		this.clause = clause;
		
		this.children.clear();
		this.children.add(tableNode);
		this.children.add(clause);
		
		return this;
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
				
				FilteredTable combinedTable = new FilteredTable().filter(rowFormat);
				combinedTable.setSource(table.getName(), table);
				
				combinedTable.setClause(clause);
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
		
		StringBuilder sb = new StringBuilder();
		
		sb.append(tableNode.generateCode()); 
		sb.append(" join ");
		sb.append(joinedTable.generateCode());
		sb.append(" on ");
		sb.append(clause.generateCode());
		
		return sb.toString();
	}
	
	@Override
	public String toString() {
		
		return generateCode();
	}
}
