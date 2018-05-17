package com.skellix.database.session;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import com.skellix.database.table.query.QueryNodeParser;
import com.skellix.database.table.query.exception.QueryParseException;
import com.skellix.database.table.query.node.QueryNode;

public class Session implements AutoCloseable {
	
	private boolean hasWritePermission = false;
	private List<Lock> locksHeld = new ArrayList<>();
	private Path startDir = Paths.get(".").normalize();
	public Map<String, Object> variables = new HashMap<>();

	private Session(boolean hasWritePermission) {
		
		this.hasWritePermission = hasWritePermission;
	}
	
	public static Session createNewSession(boolean hasWritePermission) {
		
		Session session = new Session(hasWritePermission);
		
		return session;
	}
	
	public boolean hasWritePermission() {
		
		return hasWritePermission;
	}
	
	public Object query(String queryString) throws QueryParseException,Exception {
		
		QueryNode queryNode = QueryNodeParser.parse(queryString);
		return queryNode.query(this);
	}
	
	public Object query(QueryNode preCompiledQuery) throws QueryParseException,Exception {
		
		return preCompiledQuery.query(this);
	}
	
	public void addLock(Lock lock) {
		
		locksHeld.add(lock);
	}

	@Override
	public void close() throws Exception {
		
		for (Lock lock : locksHeld) {
			
			try {
			
				lock.unlock();
				
			}catch (IllegalMonitorStateException e) {
				
				// Do nothing: It's already unlocked
			}
		}
		
		locksHeld.clear();
	}
	
	public void setStartDirectory(Path startDir) {
		
		this.startDir = startDir;
	}
	
	public Path getStartDirectory() {
		
		return startDir;
	}

}
