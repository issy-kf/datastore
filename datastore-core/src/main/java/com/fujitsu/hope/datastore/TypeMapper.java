package com.fujitsu.hope.datastore;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.fujitsu.hope.datastore.PreparedStatementExecutor.PreparedStatementBinder;
import com.fujitsu.hope.datastore.meta.AttributeMeta;
import com.fujitsu.hope.datastore.meta.ColumnType;

class TypeMapper{
	static <T, C> void bind(PreparedStatementBinder binder, AttributeMeta<T,C> attr, T entity) {
		bind(binder, attr.columnType(), attr.get(entity));
	}
	
	static void bind(PreparedStatementBinder binder, ColumnType columnType, Object value) {
		if(columnType.equals(ColumnType.STRING)) binder.setString((String) value);
		else if (columnType.equals(ColumnType.INTEGER)) binder.setInteger((Integer)value);
		else if (columnType.equals(ColumnType.LONG)) binder.setLong((Long)value);
		else if (columnType.equals(ColumnType.BYTE)) binder.setByte((Byte)value);
		else if (columnType.equals(ColumnType.SHORT)) binder.setShort((Short)value);
		else if (columnType.equals(ColumnType.FLOAT)) binder.setFloat((Float)value);
		else if (columnType.equals(ColumnType.DOUBLE)) binder.setDouble((Double)value);
		else if (columnType.equals(ColumnType.BIG_DECIMAL)) binder.setBigDecimal((BigDecimal)value);
		else if (columnType.equals(ColumnType.BOOLEAN)) binder.setBoolean((Boolean)value);
		else throw new IllegalArgumentException(columnType+" is not supported");
	}
	
	@SuppressWarnings("unchecked")
	static <E,A> void getValues(ResultSet rs, AttributeMeta<E,A> attr, E entity) throws SQLException {
		attr.set(entity, (A)getValues(rs, attr.columnType(), attr.columnName()));
	}
	
	static Object getValues(ResultSet rs, ColumnType columnType, String columnName) throws SQLException {
		if (columnType.equals(ColumnType.STRING)) return rs.getString(columnName);
		else if (columnType.equals(ColumnType.INTEGER)) return rs.getInt(columnName);
		else if (columnType.equals(ColumnType.LONG)) return rs.getLong(columnName);
		else if (columnType.equals(ColumnType.BYTE)) return rs.getByte(columnName);
		else if (columnType.equals(ColumnType.SHORT)) return rs.getShort(columnName);
		else if (columnType.equals(ColumnType.FLOAT)) return rs.getFloat(columnName);
		else if (columnType.equals(ColumnType.DOUBLE)) return rs.getDouble(columnName);
		else if (columnType.equals(ColumnType.BIG_DECIMAL)) return rs.getBigDecimal(columnName);
		else if (columnType.equals(ColumnType.BOOLEAN)) return rs.getBoolean(columnName);
		else throw new IllegalArgumentException(columnType+" is not supported");
	}
}