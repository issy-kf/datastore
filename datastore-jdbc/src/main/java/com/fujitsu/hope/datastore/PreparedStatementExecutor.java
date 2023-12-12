package com.fujitsu.hope.datastore;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

/**
 * TableMeta class is infomations of table for Relational Database 
 * @author takayama
 */
class PreparedStatementExecutor {
	private final PreparedStatementProvider provider; 
	private final ExecutorService executorService;
	private final ExecutorService fetcherService;
	private final Connection connection;
	private static ExecutorService createExecutorService(final String threadName) {
		return Executors.newSingleThreadExecutor(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, threadName);
			}
		}); 
	}
	
	PreparedStatementExecutor(String executorName, Connection connection){
		this.executorService = createExecutorService(PreparedStatementExecutor.class.getSimpleName()+":"+executorName+":"+"#execute");
		this.fetcherService = createExecutorService(PreparedStatementExecutor.class.getSimpleName()+":"+executorName+":"+"#fetch");
		this.connection = connection;
		this.provider = new PreparedStatementProvider(this);
	}
	
	private Future<PreparedStatement> preparedStatement(String sql){
		return this.executorService.submit(new Callable<PreparedStatement>() {
			@Override
			public PreparedStatement call() throws Exception {
				return connection.prepareStatement(sql);
			}
		});
	}
	
	private Future<Integer> executeUpdate (PreparedStatement ps) {
		return this.executorService.submit(new Callable<Integer>() {
			@Override
			public Integer call() throws Exception {
				return ps.executeUpdate();
			}
		});
	}
	
	private Future<ResultSetFetcher> executeQuery (PreparedStatement ps, PreparedStatementExecutor executor) {
		return this.executorService.submit(new Callable<ResultSetFetcher>() {
			@Override
			public ResultSetFetcher call() throws Exception {
				ResultSet rs = ps.executeQuery();
				return new ResultSetFetcher(rs, executor);
			}
		});
	}
	
	private Future<Boolean> nextResultSet(ResultSet rs){
		return this.fetcherService.submit(new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				boolean hasNext = rs.next(); 
				return hasNext;
			}
		});
	}
	
	private Future<Void> closeResultSet(ResultSet rs){
		return this.fetcherService.submit(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				rs.close();
				return null;
			}
		});
	}

	PreparedStatementBuilder table(TableMeta table) {
		return new PreparedStatementBuilder(this.provider.table(table), this);
	}
	
	class PreparedStatementBuilder {
		private final PreparedStatementTableCache psTable;
		private final PreparedStatementExecutor executor;
		PreparedStatementBuilder (PreparedStatementTableCache psTable, PreparedStatementExecutor executor){
			this.psTable = psTable;
			this.executor = executor;
		}
		PreparedStatementBinder insert(){
			return new PreparedStatementBinder(this.psTable.insert(), this.executor);
		}
		PreparedStatementBinder update(){
			return new PreparedStatementBinder(this.psTable.update(), this.executor);
		}
		PreparedStatementBinder delete(){
			return new PreparedStatementBinder(this.psTable.delete(), this.executor);
		}
		PreparedStatementBinder getEntity(){
			return new PreparedStatementBinder(this.psTable.entity(), this.executor);
		}
	}
	
	interface ResultSetResolver<T> {
		T resolve(ResultSet rs);
	}
	
	class ResultSetFetcher {
		private final ResultSet resultSet;
		private final PreparedStatementExecutor executor;
		private boolean hasNext = false;
		
		ResultSetFetcher(ResultSet resultSet, PreparedStatementExecutor executor){
			this.resultSet = resultSet;
			this.executor = executor;
			fetch();
		}
		
		void fetch(){
			try {
				this.hasNext = this.executor.nextResultSet(this.resultSet).get();
			} catch (InterruptedException | ExecutionException e) {
				throw new PreparedStatementExecutorException(e);
			}
			if (!this.hasNext) close();
		}
		
		void close(){
			try {
				this.executor.closeResultSet(this.resultSet).get();
			} catch (InterruptedException | ExecutionException e) {
				throw new PreparedStatementExecutorException(e);
			}
		}
		
		<T> Iterator<T> asIterator(ResultSetResolver<T> resolver){
			return new Iterator<T>() {
				@Override
				public boolean hasNext() {
					return hasNext;
				}

				@Override
				public T next() {
					T value = resolver.resolve(resultSet);
					fetch();
					return value;
				}
			};
		}
	}
	
	class PreparedStatementBinder {
		private final PreparedStatement ps;
		private final PreparedStatementExecutor executor;
		private int parameterIndex = 1;
		PreparedStatementBinder(PreparedStatement ps, PreparedStatementExecutor executor){
			this.ps = ps;
			this.executor = executor;
		}
		Future<Integer> executeUpdate () {
			return executor.executeUpdate(this.ps);
		}

		<T> Future<ResultSetFetcher> executeQuery () {
			return executor.executeQuery(this.ps, this.executor);
		}
		
		PreparedStatementBinder set(String value) {
			try {
				ps.setString(parameterIndex++, value);
			} catch (SQLException e) {
				throw new PreparedStatementExecutorException(e);
			}
			return this;
		}
		PreparedStatementBinder set(Byte value){
			try {
				ps.setByte(parameterIndex++, value);
			} catch (SQLException e) {
				throw new PreparedStatementExecutorException(e);
			}
			return this;
		}
		PreparedStatementBinder set(Integer value){
			try {
				ps.setInt (parameterIndex++, value);
			} catch (SQLException e) {
				throw new PreparedStatementExecutorException(e);
			}
			return this;
		}
		PreparedStatementBinder set(Long value){
			try {
				ps.setLong (parameterIndex++, value);
			} catch (SQLException e) {
				throw new PreparedStatementExecutorException(e);
			}
			return this;
		}
		PreparedStatementBinder set(Float value){
			try {
				ps.setFloat(parameterIndex++, value);
			} catch (SQLException e) {
				throw new PreparedStatementExecutorException(e);
			}
			return this;
		}
		PreparedStatementBinder set(Double value){
			try {
				ps.setDouble(parameterIndex++, value);
			} catch (SQLException e) {
				throw new PreparedStatementExecutorException(e);
			}
			return this;
		}
		PreparedStatementBinder set(Boolean value){
			try {
				ps.setBoolean(parameterIndex++, value);
			} catch (SQLException e) {
				throw new PreparedStatementExecutorException(e);
			}
			return this;
		}
		PreparedStatementBinder set(BigDecimal value){
			try {
				ps.setBigDecimal(parameterIndex++, value);
			} catch (SQLException e) {
				throw new PreparedStatementExecutorException(e);
			}
			return this;
		}
		PreparedStatementBinder set(java.sql.Date value){
			try {
				ps.setDate(parameterIndex++, value);
			} catch (SQLException e) {
				throw new PreparedStatementExecutorException(e);
			}
			return this;
		}
		PreparedStatementBinder set(Blob value){
			try {
				ps.setBlob(parameterIndex++, value);
			} catch (SQLException e) {
				throw new PreparedStatementExecutorException(e);
			}
			return this;
		}
		PreparedStatementBinder set(byte[] value){
			try {
				ps.setBytes(parameterIndex++, value);
			} catch (SQLException e) {
				throw new PreparedStatementExecutorException(e);
			}
			return this;
		}
	}
	
	class PreparedStatementProvider{
		private final PreparedStatementFactory factory;
		private final Map<String, PreparedStatementTableCache> cache;
		PreparedStatementProvider(PreparedStatementExecutor executor){
			this.factory = new PreparedStatementFactory(executor);
			this.cache = new ConcurrentHashMap<String, PreparedStatementTableCache>();
		}
		PreparedStatementTableCache table(TableMeta meta){
			PreparedStatementTableCache table = cache.get(meta.tableName());
			if (table == null) {
				table = new PreparedStatementTableCache(meta, this.factory);
				cache.put(meta.tableName(), table);
			}
			return table;
		}
	}
	
	class PreparedStatementTableCache{
		private final TableMeta meta;
		private final Map<PreparedStatementType, PreparedStatement> dmlMap;
		private final Map<String, PreparedStatement> selectMap;
		private final PreparedStatementFactory factory;

		PreparedStatementTableCache(TableMeta meta, PreparedStatementFactory factory){
			this.meta = meta;
			this.factory = factory;
			this.dmlMap= new ConcurrentHashMap<PreparedStatementType, PreparedStatement>();
			this.selectMap= new ConcurrentHashMap<String, PreparedStatement>();
		}
		
		private PreparedStatement dml(PreparedStatementType type){
			PreparedStatement ps = dmlMap.get(type);
			if (ps == null) {
				ps = createStatement(type);
				dmlMap.put(type, ps);
			}
			return ps;
		}
		
		PreparedStatement insert() {return dml(PreparedStatementType.INSERT);}
		PreparedStatement delete() {return dml(PreparedStatementType.DELETE);}
		PreparedStatement update() {return dml(PreparedStatementType.UPDATE);}
		PreparedStatement entity() {return dml(PreparedStatementType.ENTITY);}

		PreparedStatement select(String queryStatement){
			PreparedStatement ps = selectMap.get(queryStatement);
			if (ps == null) {
				ps = factory.selectKeys(meta, queryStatement);
				selectMap.put(queryStatement, ps);
			}
			return ps;
		}
		
		private PreparedStatement createStatement(PreparedStatementType type){
			if (type == PreparedStatementType.INSERT) return factory.insert(this.meta);
			else if (type == PreparedStatementType.UPDATE) return factory.update(this.meta);
			else if (type == PreparedStatementType.DELETE) return factory.delete(this.meta);
			else if (type == PreparedStatementType.ENTITY) return factory.getEntity(this.meta);
			else throw new UnsupportedOperationException("type[" + type + "] is not supported.");
		}
	}
	
	/**
	 */
	enum PreparedStatementType{
		UPDATE, INSERT, DELETE, ENTITY
	}
	
	class PreparedStatementFactory{
		PreparedStatementExecutor executor;
		PreparedStatementFactory(PreparedStatementExecutor executor){
			this.executor = executor;
		}
		
		PreparedStatement update(TableMeta meta){
			return prepare(DmlStatementGenerator.update(meta));
		}
		
		PreparedStatement insert(TableMeta meta){
			return prepare(DmlStatementGenerator.insert(meta));
		}
		
		PreparedStatement delete(TableMeta meta){
			return prepare(DmlStatementGenerator.delete(meta));
		}
		
		PreparedStatement getEntity(TableMeta meta){
			return prepare(DmlStatementGenerator.getEntity(meta));
		}
		
		PreparedStatement selectKeys(TableMeta meta, String option){
			return prepare(DmlStatementGenerator.selectKeys(meta, option));
		}
		
		PreparedStatement prepare(String sql){
			try {
				return this.executor.preparedStatement(sql).get();
			} catch (InterruptedException | ExecutionException e) {
				throw new PreparedStatementExecutorException(e);
			}
		}
	}

	private Future<Void> closeConnection (Connection connection) {
		return this.executorService.submit(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				connection.close();
				return null;
			}
		});
	}

	private Future<Void> commitConnection (Connection connection) {
		return this.executorService.submit(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				connection.commit();;
				return null;
			}
		});
	}

	private Future<Void> rollbackConnection (Connection connection) {
		return this.executorService.submit(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				connection.rollback();;
				return null;
			}
		});
	}
	
	PreparedStatementExecutorTransaction transaction() {
		return new PreparedStatementExecutorTransaction(this, this.connection);
	}
	
	class PreparedStatementExecutorTransaction {
		private final PreparedStatementExecutor executor;
		private final Connection connection;
		PreparedStatementExecutorTransaction(PreparedStatementExecutor executor, Connection connection){
			this.executor = executor;
			this.connection = connection;
		}
		void commit() {
			try {
				this.executor.commitConnection(connection).get();
			} catch (InterruptedException | ExecutionException e) {
				throw new PreparedStatementExecutorException(e);
			}
		}
		void rollback() {
			try {
				this.executor.rollbackConnection(connection).get();
			} catch (InterruptedException | ExecutionException e) {
				throw new PreparedStatementExecutorException(e);
			}
		}
		void close() {
			try {
				this.executor.closeConnection(connection).get();
			} catch (InterruptedException | ExecutionException e) {
				throw new PreparedStatementExecutorException(e);
			}
		}
	}
	
	@SuppressWarnings("serial")
	class PreparedStatementExecutorException extends RuntimeException{
		PreparedStatementExecutorException(Throwable e){
			super("Illegal PreparedStatement", e);
		}
		PreparedStatementExecutorException(String sql, Throwable e){
			super("Illegal Preparedstatement SQL:["+sql+"]", e);
		}
	}

}
