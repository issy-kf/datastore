package com.fujitsu.hope.datastore;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fujitsu.hope.datastore.PreparedStatementExecutor.PreparedStatementBinder;
import com.fujitsu.hope.datastore.PreparedStatementExecutor.ResultSetFetcher;
import com.fujitsu.hope.datastore.PreparedStatementExecutor.ResultSetResolver;
import com.fujitsu.hope.datastore.meta.AttributeMeta;
import com.fujitsu.hope.datastore.meta.EntityMeta;
import com.fujitsu.hope.datastore.meta.Key;

public class DataShelf<T> {
	private final PreparedStatementExecutor writerExecutor;
	private final PreparedStatementExecutor readerExecutor;
	private final EntityMeta<T> entityMeta; 
	private final TableMeta tableMeta;
	private final EntityResolver<T> entityResolver;
	DataShelf(EntityMeta<T> entityMeta, String suffix, PreparedStatementExecutor writerExecutor, PreparedStatementExecutor readerExecutor){
		this.writerExecutor = writerExecutor;
		this.readerExecutor = readerExecutor;
		this.entityMeta = entityMeta;
		this.tableMeta = TableMeta.create(entityMeta, suffix);
		this.entityResolver = new EntityResolver<T>(entityMeta);
	}
	
	public Future<UpdateResult> insert(T entity) {
		PreparedStatementBinder binder = this.writerExecutor.table(tableMeta).insert();
		for (AttributeMeta<T, ?> attr : entityMeta.keys()) TypeMapper.bind(binder, attr, entity);
		for (AttributeMeta<T, ?> attr : entityMeta.attributes()) TypeMapper.bind(binder, attr, entity);
		return new FutureUpdateResult(binder.executeUpdate());
	}
	
	public Future<UpdateResult> update(T entity) {
		PreparedStatementBinder binder = this.writerExecutor.table(tableMeta).update();
		for (AttributeMeta<T, ?> attr : entityMeta.attributes()) TypeMapper.bind(binder, attr, entity);
		for (AttributeMeta<T, ?> attr : entityMeta.keys()) TypeMapper.bind(binder, attr, entity);
		return new FutureUpdateResult(binder.executeUpdate());
	}

	public Future<UpdateResult> delete(Key<T> key) {
		PreparedStatementBinder binder = this.writerExecutor.table(tableMeta).delete();
		AttributeMeta<T,?>[] keymeta = key.meta().keys();
		for (int i=0; i<keymeta.length; i++) TypeMapper.bind(binder, keymeta[i].columnType(), key.values()[i]);
		return new FutureUpdateResult(binder.executeUpdate());
	}
	
	public Future<T> entityOf(Key<T> key) {
		PreparedStatementBinder binder = this.readerExecutor.table(tableMeta).getEntity();
		AttributeMeta<T,?>[] keymeta = key.meta().keys();
		for (int i=0; i<keymeta.length; i++) TypeMapper.bind(binder, keymeta[i].columnType(), key.values()[i]);
		return new FutureSingleValue<T>(entityResolver, binder.executeQuery());
	}

	class EntityResolver<ET> implements ResultSetResolver<ET>{
		private final EntityMeta<ET> entityMeta;
		EntityResolver(EntityMeta<ET> entityMeta){
			this.entityMeta = entityMeta;
		}

		public ET resolve(ResultSet rs) {
			ET entity = entityMeta.newInstance();
			try {
				for (AttributeMeta<ET, ?> attr : entityMeta.keys()) { TypeMapper.getValues(rs, attr, entity);} 
				for (AttributeMeta<ET, ?> attr : entityMeta.attributes()) {TypeMapper.getValues(rs, attr, entity);} 
				return entity;
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	public class FutureSingleValue<ET> implements Future<ET>{
		private final EntityResolver<ET> entityResolver;
		private final Future<ResultSetFetcher> future;

		FutureSingleValue(EntityResolver<ET> entityResolver, Future<ResultSetFetcher> future){
			this.entityResolver = entityResolver;
			this.future = future;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return this.future.cancel(mayInterruptIfRunning);
		}

		@Override
		public boolean isCancelled() {
			return this.future.isCancelled();
		}

		@Override
		public boolean isDone() {
			return this.future.isDone();
		}

		@Override
		public ET get() throws InterruptedException, ExecutionException {
			return singleValue(this.future.get());
		}

		@Override
		public ET get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			return singleValue(this.future.get(timeout, unit));
		}
		
		private ET singleValue(ResultSetFetcher resultSetFetcher) {
			Iterator<ET> ite = resultSetFetcher.asIterator(entityResolver);
			final ET ret;
			if (ite.hasNext()) ret = ite.next();
			else ret = null;
			if (ite.hasNext()) resultSetFetcher.close();
			return ret;
		}
	}
	
	public enum UpdateResult {SUCCESS, NOT_EXITED, UNEXPECTED}

	class FutureUpdateResult implements Future<UpdateResult>{
		private final Future<Integer> future;
		FutureUpdateResult(Future<Integer> future){
			this.future = future;
		}
		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return this.future.cancel(mayInterruptIfRunning);
		}

		@Override
		public boolean isCancelled() {
			return this.future.isCancelled();
		}

		@Override
		public boolean isDone() {
			return this.future.isDone();
		}
		
		private static UpdateResult toResultUpdate(int ret) {
			if(ret == 1) return UpdateResult.SUCCESS;
			else if (ret == 0) return UpdateResult.NOT_EXITED;
			else return UpdateResult.UNEXPECTED;
		}
		
		@Override
		public UpdateResult get() throws InterruptedException, ExecutionException {
			return toResultUpdate(this.future.get());
		}

		@Override
		public UpdateResult get(long timeout, TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {
			return toResultUpdate(this.future.get(timeout, unit));
		}

	}
}