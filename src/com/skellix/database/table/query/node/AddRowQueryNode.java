package com.skellix.database.table.query.node;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;

import com.skellix.database.row.RowFormat;
import com.skellix.database.row.TableRow;
import com.skellix.database.session.IllegalWriteException;
import com.skellix.database.session.Session;
import com.skellix.database.table.ExperimentalTable;
import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class AddRowQueryNode extends QueryNode {
	
	private QueryNode rowQuery;
	private QueryNode destQuery;
	
	public AddRowQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
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
		
		if (nextNode == null || !(nextNode instanceof QueryNode)) {
			
			String errorString = String.format("ERROR: expected map after '%s' at %d, %d"
					, replaceNode.getLabel(), replaceNode.line, replaceNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		QueryNode nextMapNode = (QueryNode) nextNode;
		
		if (!List.class.isAssignableFrom(nextMapNode.resultType)) {
			
			String errorString = String.format("ERROR: expected map after '%s' at %d, %d"
					, replaceNode.getLabel(), nextMapNode.line, nextMapNode.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		addResultOf(nextMapNode, (QueryNode) previousQueryNode);
		replaceNode.replaceWith(this);
		
		previousNode.removeFromParent();
		nextNode.removeFromParent();
		
		copyValuesFrom(replaceNode);
		
		return this;
	}

	public AddRowQueryNode addResultOf(QueryNode rowQuery, QueryNode destQuery) {
		
		this.rowQuery = rowQuery;
		this.destQuery = destQuery;
		
		this.children.clear();
		this.children.add(rowQuery);
		this.children.add(destQuery);
		return this;
	}
	
	@Override
	public Object query(Session session) throws Exception {
		
		if (!session.hasWritePermission()) {
			
			throw new IllegalWriteException("Tried to write data without write access");
		}
		
		Object mappings = rowQuery.query(session);
		List<EntryQueryNode> rowMappings = (List<EntryQueryNode>) mappings;
		
		Object tableQueryResult = destQuery.query(session);
		ExperimentalTable table = (ExperimentalTable) tableQueryResult;
		RowFormat rowFormat = table.rowFormat;
		
		Lock lock = table.getWriteLock();
		session.addLock(lock);
		
		try {
			
			lock.lock();
		
			TableRow row = table.addRow(session);
			
			for (EntryQueryNode entryNode : rowMappings) {
				
				Object entryObj = entryNode.query(session);
				
				if (entryObj instanceof Entry) {
					
					Entry<StringQueryNode, QueryNode> entry = (Entry<StringQueryNode, QueryNode>) entryObj;
					String key = (String) entry.getKey().query(session);
					Object value = entry.getValue().query(session);
					Object casted = rowFormat.columnTypes.get(key).cast(value);
					row.columns.get(rowFormat.columnIndexes.get(key)).set(casted);
				}
			}
		
		} catch (Exception e) {
			
			throw e;
			
		} finally {
			
			lock.unlock();
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
