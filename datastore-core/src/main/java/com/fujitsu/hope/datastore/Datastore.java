package com.fujitsu.hope.datastore;

import java.sql.Connection;

import com.fujitsu.hope.datastore.meta.EntityMeta;

class Datastore {
	private final PreparedStatementExecutor writerExecutor;  
	private final PreparedStatementExecutor readerExecutor;

	Datastore(String serviceName, Connection connection){
		this.writerExecutor = new PreparedStatementExecutor(serviceName+"(writer)", connection);
		this.readerExecutor = new PreparedStatementExecutor(serviceName+"(reader)", connection);
	}

	Datastore(String serviceName, Connection writerConnection, Connection readerConnection){
		this.writerExecutor = new PreparedStatementExecutor(serviceName+"(writer)", writerConnection);
		this.readerExecutor = new PreparedStatementExecutor(serviceName+"(reader)", readerConnection);
	}
	
	<T> DataShelf<T> shelf(EntityMeta<T> meta){
		return shelf(meta, "");
	}
	
	<T> DataShelf<T> shelf(EntityMeta<T> meta, String suffix){
		return new DataShelf<T>(meta, suffix, this.writerExecutor, this.readerExecutor);
	}
	
	PreparedStatementExecutor writer() {
		return this.writerExecutor;
	}
	
	PreparedStatementExecutor reader() {
		return this.readerExecutor;
	}
}