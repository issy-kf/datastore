package com.fujitsu.hope.ds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	private final ColumnMeta<?>[] properties;
	/**
	 * Property of Primary Key
	 */
	private final ColumnMeta<?>[] keys;
	private final IndexMeta[] indexes;
	private final Map<String, ColumnMetaType<?>> metaTypeMap = 
			new HashMap<String, ColumnMetaType<?>>();
	TableMeta(String tableName, ColumnMeta<?>[] keys, ColumnMeta<?>[] columns, IndexMeta[] indexes){
		this.tableName = tableName;
		this.keys = keys;
		this.properties = columns;	
		this.indexes = indexes;
		for(ColumnMeta<?> meta : columns) metaTypeMap.put(meta.name, meta.type);
		for(ColumnMeta<?> meta : keys) metaTypeMap.put(meta.name, meta.type);
	}
	private static String[] toString(ColumnMeta<?>[] meta){
		String[] strings = new String[meta.length];
		for(int i=0; i<strings.length; i++)
			strings[i] = meta[i].name;
		return strings;
	}
	String tableName(){
		return tableName;
	}
	String[] keyNames(){
		return toString(keys);
	}
	ColumnMeta<?>[] keys() {
		return keys;
	}
	boolean isKey(ColumnMeta<?> column){
		for(ColumnMeta<?> meta : keys)
			if (meta.getName().equals(column.getName()))
				return true;
		return false;
	}
	String[] columnNames(){
		return toString(properties);
	}
	ColumnMeta<?>[] properties() {
		return properties;
	}
	ColumnMeta<?>[] allColumn(){
		ColumnMeta<?>[] ret = new ColumnMeta<?>[keys.length+properties.length];
		for(int i=0; i<keys.length; i++) ret[i] = keys[i];
		for(int i=0; i<properties.length; i++) ret[i+keys.length] = properties[i];
		return ret;
	}
	IndexMeta[] indexes(){
		return indexes;
	}
	ColumnMetaType<?> metaTypeOf(String name){
		return metaTypeMap.get(name);
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		TableMeta other = (TableMeta) obj;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		if (!Arrays.equals(properties, other.properties))
			return false;
		if (!Arrays.equals(keys, other.keys))
			return false;
		return true;
	}
	static class ColumnMetaType<T>{
		private final Class<T> classType;
		private final String name;
		private ColumnMetaType(Class<T> classType){
			this.classType = classType;
			this.name = classType.getSimpleName();
		}
		private ColumnMetaType(String name, Class<T> classType){
			this.name = name;
			this.classType = classType;
		}
		public Class<T> getClassType(){
			return classType;
		}
		public String getName(){
			return name;
		}
	}
	static class ColumnMeta<T>{
		static final ColumnMetaType<Boolean> BOOLEAN = new ColumnMetaType<Boolean>(Boolean.class);
		static final ColumnMetaType<Byte> BYTE = new ColumnMetaType<Byte>(Byte.class);
		static final ColumnMetaType<Date> DATE = new ColumnMetaType<Date>(Date.class);
		static final ColumnMetaType<Double> DOUBLE = new ColumnMetaType<Double>(Double.class);
		static final ColumnMetaType<Float> FLOAT = new ColumnMetaType<Float>(Float.class);
		static final ColumnMetaType<Integer> INTEGER = new ColumnMetaType<Integer>(Integer.class);
		static final ColumnMetaType<Long> LONG = new ColumnMetaType<Long>(Long.class);
		static final ColumnMetaType<Short> SHORT = new ColumnMetaType<Short>(Short.class);
		static final ColumnMetaType<String> STRING = new ColumnMetaType<String>(String.class);
		static final ColumnMetaType<byte[]> BYTES = new ColumnMetaType<byte[]>(byte[].class);
		private final String name;
		private final int length;
		private final int decimal;
		private final ColumnMetaType<T> type;
		ColumnMeta(String name, int length, int decimal, ColumnMetaType<T> type){
			this.name = name;
			this.type = type;
			this.length = length;
			this.decimal = decimal;
		}
		ColumnMeta(String name, int length, ColumnMetaType<T> type){
			this(name, length, 0, type);
		}
		ColumnMeta(String name, ColumnMetaType<T> type){
			this(name, 0, 0, type);
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			ColumnMeta<?> other = (ColumnMeta<?>) obj;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			return true;
		}
		String getName() {
			return name;
		}
		int getLength(){
			return length;
		}
		int getDecimal(){
			return decimal;
		}
		ColumnMetaType<?> getType() {
			return type;
		}
	}
	static class IndexMeta{
		private final String name;
		private final ColumnMeta<?>[] columns;
		private final boolean unique;
		IndexMeta(String name, boolean unique, ColumnMeta<?>...columns){
			this.name = name;
			this.columns = columns;
			this.unique = unique;
		}
		ColumnMeta<?>[] columns(){
			return columns;
		}
		String getName() {
			return name;
		}
		boolean isUnique() {
			return unique;
		}
	}
	static TableMetaBuilder table(String tableName){
		return new TableMetaBuilder(tableName);
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
		<T> TableMetaBuilder type(ColumnMetaType<T> type){
			if(isKey) 
				builder.addKey(new ColumnMeta<T>(columnName, type));
			else 
				builder.addColumn(new ColumnMeta<T>(columnName, type));
			return builder;
		}
		TableMetaBuilder asBoolean(){ return type(ColumnMeta.BOOLEAN); }
		TableMetaBuilder asByte(){ return type(ColumnMeta.BYTE); }
		TableMetaBuilder asDate(){ return type(ColumnMeta.DATE); }
		TableMetaBuilder asDouble(){ return type(ColumnMeta.DOUBLE); }
		TableMetaBuilder asFloat(){ return type(ColumnMeta.FLOAT); }
		TableMetaBuilder asInteger(){ return type(ColumnMeta.INTEGER); }
		TableMetaBuilder asLong(){ return type(ColumnMeta.LONG); }
		TableMetaBuilder asShort(){ return type(ColumnMeta.SHORT); }
		TableMetaBuilder asString(int length){ return type(ColumnMeta.STRING); }
		TableMetaBuilder asString(){ return type(ColumnMeta.STRING); }
		TableMetaBuilder asBytes(){ return type(ColumnMeta.BYTES); }
	}
	static class TableMetaBuilder{
		private final String tableName;
		private List<ColumnMeta<?>> columnList = new ArrayList<ColumnMeta<?>>();
		private List<ColumnMeta<?>> keyList = new ArrayList<ColumnMeta<?>>();
		private List<IndexMeta> indexList = new ArrayList<IndexMeta>();
		private TableMetaBuilder(String tableName){
			this.tableName = tableName;
		}
		
		private <T> void addColumn(ColumnMeta<T> meta){
			columnList.add(meta);
		}
		private <T> void addKey(ColumnMeta<T> meta){
			keyList.add(meta);
		}
		private void addIndex(IndexMeta index){
			this.indexList.add(index);
		}
		ColumnMetaBuilder key(String name){
			return new ColumnMetaBuilder(this, name, true);
		}
		ColumnMetaBuilder column(String name){
			return new ColumnMetaBuilder(this, name, false);
		}
		IndexMetaBuilder index(String name){
			return new IndexMetaBuilder(this, name);
		}
		TableMeta meta(){
			ColumnMeta<?>[] keyArray = new ColumnMeta<?>[keyList.size()];
			keyList.toArray(keyArray);
			ColumnMeta<?>[] columnArray = new ColumnMeta<?>[columnList.size()];
			columnList.toArray(columnArray);
			IndexMeta[] indexes = new IndexMeta[indexList.size()];
			indexList.toArray(indexes);
			return new TableMeta(tableName, keyArray, columnArray, indexes);
		}
	}
	static class IndexMetaBuilder{
		private final String name;
		private final TableMetaBuilder builder;
		private boolean unique = false;
		IndexMetaBuilder(TableMetaBuilder builder, String name){
			this.builder = builder;
			this.name = name;
		}
		IndexMetaBuilder unique(){
			unique = true;
			return this;
		}
		TableMetaBuilder of(String... columns){
			this.builder.addIndex(createIndexMeta(columns));
			return this.builder;
		}
		private IndexMeta createIndexMeta(String...columns){
			if (columns.length == 0)
				throw new IllegalArgumentException("columns length = 0");
			ColumnMeta<?>[] meta = new ColumnMeta[columns.length];			
			for (int i=0; i<columns.length; i++)
				meta[i] = metaOf(columns[i]);
			return new IndexMeta(name, unique, meta);
		}
		private ColumnMeta<?> metaOf(String name){
			for (ColumnMeta<?> meta : builder.columnList)
				if (meta.getName().equals(name))
					return meta;
			for (ColumnMeta<?> meta : builder.keyList)
				if (meta.getName().equals(name))
					return meta;
			throw new IllegalArgumentException("column ["+name+"] is not exists");
		}
	}
}