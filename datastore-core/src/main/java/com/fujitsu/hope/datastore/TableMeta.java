package com.fujitsu.hope.datastore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fujitsu.hope.datastore.meta.AttributeMeta;
import com.fujitsu.hope.datastore.meta.ColumnType;
import com.fujitsu.hope.datastore.meta.EntityMeta;

/**
 * TableMeta class is infomations of table for Relational Database 
 * @author takayama
 */
class TableMeta {
	/**
	 * table name
	 */
	private final String tableName;
	/**
	 * Column Names exclude Primary Key?
	 */
	private final ColumnMeta[] attributes;
	/**
	 * Property of Primary Key
	 */
	private final ColumnMeta[] keys;

	private final Map<String, ColumnType> metaTypeMap = new HashMap<String, ColumnType>();
	
	TableMeta(String tableName, ColumnMeta[] keys, ColumnMeta[] attributes){
		this.tableName = tableName;
		this.keys = keys;
		this.attributes = attributes;	
		for(ColumnMeta meta : attributes) metaTypeMap.put(meta.name, meta.type);
		for(ColumnMeta meta : keys) metaTypeMap.put(meta.name, meta.type);
	}
	
	String tableName(){
		return tableName;
	}
	
	ColumnMeta[] keys() {
		return keys;
	}

	ColumnMeta[] attributes() {
		return attributes;
	}

	static class ColumnMeta{
		private final String name;
		private final ColumnType type;

		ColumnMeta(String name, ColumnType type){
			this.name = name;
			this.type = type;
		}

		String getName() {
			return name;
		}

		ColumnType getType() {
			return type;
		}
	}

	static TableMetaBuilder table(String tableName){
		return new TableMetaBuilder(tableName);
	}
	
	static <T> TableMeta create(EntityMeta<T> entityMeta) {
		return create(entityMeta, "");
	}
	
	static <T> TableMeta create(EntityMeta<T> entityMeta, String suffix) {
		TableMeta.TableMetaBuilder builder = TableMeta.table(entityMeta.tableName() + suffix);
		for (AttributeMeta<T, ?> meta : entityMeta.keys()) builder.key(meta.columnName()).type(meta.columnType());
		for (AttributeMeta<T, ?> meta : entityMeta.attributes()) builder.attribute(meta.columnName()).type(meta.columnType());
		return builder.meta();
	}
	
	static class TableMetaBuilder{
		private final String tableName;
		private List<ColumnMeta> columnList = new ArrayList<ColumnMeta>();
		private List<ColumnMeta> keyList = new ArrayList<ColumnMeta>();
		private TableMetaBuilder(String tableName){
			this.tableName = tableName;
		}
		
		private <T> void addAttribute(ColumnMeta meta){
			columnList.add(meta);
		}
		private <T> void addKey(ColumnMeta meta){
			keyList.add(meta);
		}
		ColumnMetaBuilder key(String name){
			return new ColumnMetaBuilder(this, name, true);
		}
		ColumnMetaBuilder attribute(String name){
			return new ColumnMetaBuilder(this, name, false);
		}
		TableMeta meta(){
			if (keyList.size() == 0) 
				throw new UnsupportedOperationException(
						"TableMeta:" + tableName + "has no key"); 
			ColumnMeta[] keyArray = new ColumnMeta[keyList.size()];
			keyList.toArray(keyArray);
			ColumnMeta[] attributeArray = new ColumnMeta[columnList.size()];
			columnList.toArray(attributeArray);
			return new TableMeta(tableName, keyArray, attributeArray);
		}
	}
	
	static class ColumnMetaBuilder{
		private final boolean isKey;
		private final String columnName;
		private final TableMetaBuilder builder;
		private ColumnMetaBuilder(TableMetaBuilder builder, String columnName, boolean isKey){
			this.builder = builder;
			this.columnName = columnName;
			this.isKey = isKey;
		}
		<T> TableMetaBuilder type(ColumnType type){
			if(isKey) 
				builder.addKey(new ColumnMeta(columnName, type));
			else 
				builder.addAttribute(new ColumnMeta(columnName, type));
			return builder;
		}
	}
}