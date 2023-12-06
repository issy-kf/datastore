package com.fujitsu.hope.ds;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.junit.Test;

import com.fujitsu.hope.ds.TableMeta.ColumnMeta;
import com.fujitsu.hope.ds.TableMeta.ColumnMetaType;

public class TableMetaTest {
	/**
	 * 単独キーでのTableMetaクラスの検証 その1
	 */
	@Test
	public void testTableMetaBuildCase001(){
		TableMeta meta = 
				TableMeta.table("person")
				.key("id").type(ColumnMeta.INTEGER)
				.column("profile").type(ColumnMeta.STRING)
				.column("profile2").type(ColumnMeta.STRING)
				.meta();
		assertThat(meta.keys().length, is(1));
		assertThat(meta.properties().length, is(2));
		assertThat(meta.keyNames()[0], is("id"));
		assertThat(meta.keys()[0].getName(), is("id"));
		assertThat(meta.columnNames()[0], is("profile"));
		assertThat(meta.columnNames()[1], is("profile2"));
	}

	/**
	 * 単独キーでのTableMetaクラスの検証 その2
	 */
	@Test
	public void testTableMetaBuildCase002(){
		TableMeta meta = 
				TableMeta.table("person")
				.key("id").type(ColumnMeta.INTEGER)
				.meta();
		assertThat(meta.keys().length, is(1));
		assertThat(meta.properties().length, is(0));
		assertThat(meta.keyNames()[0], is("id"));
		assertThat(meta.keys()[0].getName(), is("id"));
		assertThat(meta.keys()[0].getType(), is(ColumnMeta.INTEGER));
		assertThat(meta.columnNames().length, is(0));
	}
	
	/**
	 * 複合キーでのTableMetaクラスの検証 その1
	 */
	@Test
	public void testTableMetaBuildCase011(){
		TableMeta meta = 
				TableMeta.table("account")
				.key("id").type(ColumnMeta.INTEGER)
				.key("email").type(ColumnMeta.STRING)
				.column("profile").type(ColumnMeta.STRING)
				.column("profile2").type(ColumnMeta.STRING)
				.meta();
		assertThat(meta.properties().length, is(2));
		assertThat(meta.keys().length, is(2));
		assertThat(meta.keyNames()[0], is("id"));
		assertThat(meta.keyNames()[1], is("email"));
		assertThat(meta.keys()[0].getName(), is("id"));
		assertThat(meta.keys()[1].getName(), is("email"));
		assertThat(meta.columnNames()[0], is("profile"));
		assertThat(meta.columnNames()[1], is("profile2"));
	}
	
	/**
	 * 複合キーでのTableMetaクラスの検証 その2
	 */
	@Test
	public void testTableMetaBuildCase012(){
		TableMeta meta = 
				TableMeta.table("account")
				.key("id").type(ColumnMeta.INTEGER)
				.key("email").type(ColumnMeta.STRING)
				.column("profile1").type(ColumnMeta.STRING)
				.column("profile2").type(ColumnMeta.STRING)
				.column("profile3").type(ColumnMeta.INTEGER)
				.meta();
		assertThat(meta.properties().length, is(3));
		assertThat(meta.keys().length, is(2));
		assertThat(meta.keyNames()[0], is("id"));
		assertThat(meta.keyNames()[1], is("email"));
		assertThat(meta.keys()[0].getName(), is("id"));
		assertThat(meta.keys()[1].getName(), is("email"));
		assertThat(meta.columnNames()[0], is("profile1"));
		assertThat(meta.columnNames()[1], is("profile2"));
		assertThat(meta.columnNames()[2], is("profile3"));
	}
	
	class BindHelper4Rs {
		private final ResultSet resultSet;
		
		BindHelper4Rs(ResultSet resultSet) {
			this.resultSet = resultSet;
		}
		
		@SuppressWarnings("unchecked")
		<T> T getValue(String columnName, ColumnMetaType<T> type)
				throws SQLException {
			Object value = null;
			if (type.equals(ColumnMeta.STRING))
				value = resultSet.getString(columnName);
			if (type.equals(ColumnMeta.INTEGER))
				value = resultSet.getInt(columnName);
			if (type.equals(ColumnMeta.LONG))
				value = resultSet.getLong(columnName);
			if (type.equals(ColumnMeta.SHORT))
				value = resultSet.getShort(columnName);
			if (type.equals(ColumnMeta.DOUBLE))
				value = resultSet.getDouble(columnName);
			if (type.equals(ColumnMeta.FLOAT))
				value = resultSet.getFloat(columnName);
			if (type.equals(ColumnMeta.BYTE))
				value = resultSet.getByte(columnName);
			if (type.equals(ColumnMeta.DATE))
				value = new Date(resultSet.getDate(columnName).getTime());
			if (value != null)
				return (T) value;
			throw new IllegalArgumentException("unsupport class Type:"
					+ type.getName());
		}
	}
}
