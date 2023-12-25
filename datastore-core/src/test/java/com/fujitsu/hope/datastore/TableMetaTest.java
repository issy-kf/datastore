package com.fujitsu.hope.datastore;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.fujitsu.hope.datastore.meta.ColumnType;

public class TableMetaTest {
	/**
	 * 単独キーでのTableMetaクラスの検証 その1
	 */
	@Test
	public void testTableMetaBuildCase001(){
		TableMeta meta = 
				TableMeta.table("person")
				.key("id").type(ColumnType.INTEGER)
				.attribute("profile").type(ColumnType.STRING)
				.attribute("profile2").type(ColumnType.STRING)
				.meta();
		assertThat(meta.keys().length, is(1));
		assertThat(meta.attributes().length, is(2));
		assertThat(meta.keys()[0].getName(), is("id"));
		assertThat(meta.attributes()[0].getName(), is("profile"));
		assertThat(meta.attributes()[1].getName(), is("profile2"));
	}

	/**
	 * 単独キーでのTableMetaクラスの検証 その2
	 */
	@Test
	public void testTableMetaBuildCase002(){
		TableMeta meta = 
				TableMeta.table("person")
				.key("id").type(ColumnType.INTEGER)
				.meta();
		assertThat(meta.keys().length, is(1));
		assertThat(meta.attributes().length, is(0));
		assertThat(meta.keys()[0].getName(), is("id"));
		assertThat(meta.keys()[0].getName(), is("id"));
		assertThat(meta.keys()[0].getType(), is(ColumnType.INTEGER));
		assertThat(meta.attributes().length, is(0));
	}
	
	/**
	 * 複合キーでのTableMetaクラスの検証 その1
	 */
	@Test
	public void testTableMetaBuildCase011(){
		TableMeta meta = 
				TableMeta.table("account")
				.key("id").type(ColumnType.INTEGER)
				.key("email").type(ColumnType.STRING)
				.attribute("profile").type(ColumnType.STRING)
				.attribute("profile2").type(ColumnType.STRING)
				.meta();
		assertThat(meta.attributes().length, is(2));
		assertThat(meta.keys().length, is(2));
		assertThat(meta.keys()[0].getName(), is("id"));
		assertThat(meta.keys()[1].getName(), is("email"));
		assertThat(meta.attributes()[0].getName(), is("profile"));
		assertThat(meta.attributes()[1].getName(), is("profile2"));
	}
	
	/**
	 * 複合キーでのTableMetaクラスの検証 その2
	 */
	@Test
	public void testTableMetaBuildCase012(){
		TableMeta meta = 
				TableMeta.table("account")
				.key("id").type(ColumnType.INTEGER)
				.key("email").type(ColumnType.STRING)
				.attribute("profile1").type(ColumnType.STRING)
				.attribute("profile2").type(ColumnType.STRING)
				.attribute("profile3").type(ColumnType.INTEGER)
				.meta();
		assertThat(meta.attributes().length, is(3));
		assertThat(meta.keys().length, is(2));
		assertThat(meta.keys()[0].getName(), is("id"));
		assertThat(meta.keys()[1].getName(), is("email"));
		assertThat(meta.attributes()[0].getName(), is("profile1"));
		assertThat(meta.attributes()[1].getName(), is("profile2"));
		assertThat(meta.attributes()[2].getName(), is("profile3"));
	}
}
