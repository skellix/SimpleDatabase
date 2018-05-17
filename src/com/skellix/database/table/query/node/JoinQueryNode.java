package com.skellix.database.table.query.node;

import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;

import com.skellix.database.session.Session;
import com.skellix.database.table.CombinedTable;
import com.skellix.database.table.ExperimentalTable;
import com.skellix.database.table.RowFormat;
import com.skellix.database.table.TableRow;
import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class JoinQueryNode extends QueryNode {
	
	private QueryNode originTable;
	private QueryNode joinedTable;
	private QueryNode clause;

	public JoinQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		TreeNode previousNode = replaceNode.getPreviousSibling();
		
		if (previousNode == null || !(previousNode instanceof TableQueryNode || previousNode instanceof JoinQueryNode)) {
			
			String errorString = String.format("ERROR: expected table before '%s' at %d, %d"
					, replaceNode.getLabel(), replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		TreeNode nextNode = replaceNode.getNextSibling();
		
		if (nextNode == null || !(nextNode instanceof TableQueryNode || nextNode instanceof JoinQueryNode)) {
			
			String errorString = String.format("ERROR: expected table after '%s' at %d, %d"
					, replaceNode.getLabel(), replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		TreeNode nextNextNode = nextNode.getNextSibling();
		
		if (nextNextNode == null || nextNextNode.hasChildren() || !nextNextNode.getLabel().equals("on")) {
			
			String errorString = String.format("ERROR: expected table after '%s' at %d, %d"
					, replaceNode.getLabel(), replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		TreeNode clauseNode = nextNextNode.getNextSibling();
		
		if (clauseNode == null) {
			
			String errorString = String.format("ERROR: expected table after '%s' at %d, %d"
					, replaceNode.getLabel(), replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		
		joinTables((QueryNode) previousNode, (QueryNode) nextNode, (QueryNode) clauseNode);
		replaceNode.replaceWith(this);
		copyValuesFrom(replaceNode);
		parent = replaceNode.parent;
		
		previousNode.removeFromParent();
		nextNode.removeFromParent();
		nextNextNode.removeFromParent();
		clauseNode.removeFromParent();
		
		return this;
	}

	private JoinQueryNode joinTables(QueryNode originTable, QueryNode joinedTable, QueryNode clause) {
		
		this.originTable = originTable;
		this.joinedTable = joinedTable;
		this.clause = clause;
		
		this.children.clear();
		this.children.add(originTable);
		this.children.add(joinedTable);
		this.children.add(clause);
		
		return this;
	}

	@Override
	public Object query(Session session) throws Exception {
		
		Object tableQueryResult1 = originTable.query(session);
		
		if (tableQueryResult1 instanceof ExperimentalTable) {
			
			ExperimentalTable table1 = (ExperimentalTable) tableQueryResult1;
			RowFormat rowFormat1 = table1.rowFormat;
			
			Object tableQueryResult2 = joinedTable.query(session);
			
			if (tableQueryResult2 instanceof ExperimentalTable) {
				
				ExperimentalTable table2 = (ExperimentalTable) tableQueryResult2;
				RowFormat rowFormat2 = table2.rowFormat;
				
				Lock lock1 = table1.getReadLock();
				Lock lock2 = table2.getReadLock();
				session.addLock(lock1);
				session.addLock(lock2);
				
				try {
					
					lock1.lock();
					lock2.lock();
					
					CombinedTable combinedTable = new CombinedTable().combine(rowFormat1, rowFormat2);
					
					combinedTable.setSource(table1.getName(), table1);
					combinedTable.setSource(table2.getName(), table2);
					
					combinedTable.setClause(clause);
					combinedTable.setSession(session);
					
					return combinedTable;
				
				} catch (Exception e) {
					
					throw e;
					
				} finally {
					
					lock1.unlock();
					lock2.unlock();
				}
			}
		}
		return null;
	}

	@Override
	public String generateCode() {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append(originTable.generateCode()); 
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
