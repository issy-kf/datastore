package com.fujitsu.hope.ds;

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


public class PreparedStatementExecutor {
	private final PreparedStatementProvider provider; 
	private final ExecutorService service;

	private static ExecutorService createExecutorService(final String threadName) {
		return Executors.newSingleThreadExecutor(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, threadName);
			}
		}); 
	}
	
	PreparedStatementExecutor(Connection connection){
		this.service = createExecutorService(PreparedStatementExecutor.class.getSimpleName());
		this.provider = new PreparedStatementProvider(connection, this.service);
	}

	PreparedStatementBuilder table(TableMeta table) {
		return new PreparedStatementBuilder(this.provider.table(table), this.service);
	}
	
	class PreparedStatementBuilder {
		private final PreparedStatementTableCache psTable;
		private final ExecutorService srv;
		PreparedStatementBuilder (PreparedStatementTableCache psTable, ExecutorService service){
			this.psTable = psTable;
			this.srv = service;
		}
		PreparedStatementBinder insert() throws PreparedStatementProviderException {
			return new PreparedStatementBinder( this.psTable.insert(), this.srv);
		}
		PreparedStatementBinder update() throws PreparedStatementProviderException {
			return new PreparedStatementBinder( this.psTable.update(), this.srv);
		}
		PreparedStatementBinder delete() throws PreparedStatementProviderException {
			return new PreparedStatementBinder( this.psTable.delete(), this.srv);
		}
		PreparedStatementBinder getEntity() throws PreparedStatementProviderException {
			return new PreparedStatementBinder( this.psTable.entity(), this.srv);
		}
	}
	
	interface ResultSetResolver<T> {
		T resolve(ResultSet rs);
	}
	
	class ResultSetAsIterator<T> implements Iterator<T>{
		private final ResultSetResolver<T> resolver;
		private final ResultSet resultSet;
		private boolean hasNext = false;
		ResultSetAsIterator(ResultSet resultSet, ResultSetResolver<T> resolver) throws SQLException{
			this.resolver = resolver;
			this.resultSet = resultSet;
			this.hasNext = resultSet.next();
			if (!hasNext) resultSet.close();
		}
		@Override
		public boolean hasNext() {
			return hasNext;
		}

		@Override
		public T next() {
			T ret = resolver.resolve(this.resultSet);
			try {
				this.hasNext = this.resultSet.next();
				if (!hasNext) resultSet.close();
			} catch (SQLException e) {
				throw new RuntimeException();
			}
			return ret;
		}
	} 
	
	class PreparedStatementBinder {
		private final PreparedStatement ps;
		private final ExecutorService service;
		private int parameterIndex = 1;
		PreparedStatementBinder(PreparedStatement ps, ExecutorService service){
			this.ps = ps;
			this.service = service;
		}
		Future<Integer> executeUpdate () {
			return this.service.submit(new Callable<Integer>() {
				@Override
				public Integer call() throws Exception {
					return ps.executeUpdate();
				}
			});
		}

		<T> Future<Iterator<T>> executeQuery (ResultSetResolver<T> resolver) {
			return this.service.submit(new Callable<Iterator<T>>() {
				@Override
				public Iterator<T> call() throws Exception {
					return new ResultSetAsIterator<T>(ps.executeQuery(), resolver);
				}
			});
		}
		
		PreparedStatementBinder set(String value) throws SQLException {
			ps.setString(parameterIndex++, value);
			return this;
		}
		PreparedStatementBinder set(Byte value) throws SQLException {
			ps.setByte(parameterIndex++, value);
			return this;
		}
		PreparedStatementBinder set(Integer value) throws SQLException {
			ps.setInt (parameterIndex++, value);
			return this;
		}
		PreparedStatementBinder set(Long value) throws SQLException {
			ps.setLong (parameterIndex++, value);
			return this;
		}
		PreparedStatementBinder set(Float value) throws SQLException {
			ps.setFloat(parameterIndex++, value);
			return this;
		}
		PreparedStatementBinder set(Double value) throws SQLException {
			ps.setDouble(parameterIndex++, value);
			return this;
		}
		PreparedStatementBinder set(Boolean value) throws SQLException {
			ps.setBoolean(parameterIndex++, value);
			return this;
		}
		PreparedStatementBinder set(BigDecimal value) throws SQLException {
			ps.setBigDecimal(parameterIndex++, value);
			return this;
		}
		PreparedStatementBinder set(java.sql.Date value) throws SQLException {
			ps.setDate(parameterIndex++, value);
			return this;
		}
		PreparedStatementBinder set(Blob value) throws SQLException {
			ps.setBlob(parameterIndex++, value);
			return this;
		}
		PreparedStatementBinder set(byte[] value) throws SQLException {
			ps.setBytes(parameterIndex++, value);
			return this;
		}
	}
	
	class PreparedStatementProvider{
		private final PreparedStatementFactory factory;
		private final Map<String, PreparedStatementTableCache> cache;
		PreparedStatementProvider(Connection connection, ExecutorService service){
			this.factory = new PreparedStatementFactory(connection, service);
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
		
		private PreparedStatement dml(PreparedStatementType type) throws PreparedStatementProviderException{
			PreparedStatement ps = dmlMap.get(type);
			if (ps == null) {
				ps = createStatement(type);
				dmlMap.put(type, ps);
			}
			return ps;
		}
		
		PreparedStatement insert() throws PreparedStatementProviderException { return dml(PreparedStatementType.INSERT); }
		PreparedStatement delete() throws PreparedStatementProviderException { return dml(PreparedStatementType.DELETE); }
		PreparedStatement update() throws PreparedStatementProviderException { return dml(PreparedStatementType.UPDATE); }
		PreparedStatement entity() throws PreparedStatementProviderException { return dml(PreparedStatementType.ENTITY); }

		PreparedStatement select(String queryStatement) throws PreparedStatementProviderException{
			PreparedStatement ps = selectMap.get(queryStatement);
			if (ps == null) {
				ps = factory.selectKeys(meta, queryStatement);
				selectMap.put(queryStatement, ps);
			}
			return ps;
		}
		
		private PreparedStatement createStatement(PreparedStatementType type) throws PreparedStatementProviderException{
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
		private final Connection connection;
		private final DmlStatementGenerator helper;
		private final ExecutorService service;
		PreparedStatementFactory(Connection connection, ExecutorService service){
			this.connection = connection;
			helper = new DmlStatementGenerator();
			this.service = service;
		}
		
		PreparedStatement update(TableMeta meta) throws PreparedStatementProviderException{
			return prepare(helper.update(meta));
		}
		
		PreparedStatement insert(TableMeta meta) throws PreparedStatementProviderException{
			return prepare(helper.insert(meta));
		}
		
		PreparedStatement delete(TableMeta meta) throws PreparedStatementProviderException{
			return prepare(helper.delete(meta));
		}
		
		PreparedStatement getEntity(TableMeta meta) throws PreparedStatementProviderException{
			return prepare(helper.getEntity(meta));
		}
		
		PreparedStatement selectKeys(TableMeta meta, String option) throws PreparedStatementProviderException{
			return prepare(helper.selectKeys(meta, option));
		}
		
		PreparedStatement prepare(String sql) throws PreparedStatementProviderException{
			try {
				return this.service.submit(new Callable<PreparedStatement>() {
					@Override
					public PreparedStatement call() throws Exception {
						return connection.prepareStatement(sql);
					}
				}).get();
			} catch (InterruptedException e) { 
				throw new PreparedStatementProviderException(sql, e);
			} catch (ExecutionException e) { 
				throw new PreparedStatementProviderException(sql, e);
			}
		}
	}
	@SuppressWarnings("serial") 
	class PreparedStatementProviderException extends Exception{
		PreparedStatementProviderException(Throwable e){
			super("Illegal PreparedStatement", e);
		}
		PreparedStatementProviderException(String sql, Throwable e){
			super("Illegal Preparedstatement SQL:["+sql+"]", e);
		}
	}

}
