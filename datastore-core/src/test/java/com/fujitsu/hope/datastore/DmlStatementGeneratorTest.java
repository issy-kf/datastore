package com.fujitsu.hope.datastore;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.fujitsu.hope.datastore.meta.ColumnType;

public class DmlStatementGeneratorTest {
	static TableMeta person(){
		return TableMeta
				.table("person")
				.key("personId").type(ColumnType.LONG)
				.attribute("firstName").type(ColumnType.STRING)
				.attribute("familyName").type(ColumnType.STRING)
				.attribute("age").type(ColumnType.LONG)
				.meta();
	}

	@Test
	public void testDmlStatementGeneratorCasePersonSelectKey(){
		String result = DmlStatementGenerator.selectKeys(person(), "where personId=?");
		assertThat(result, is("select personId from person where personId=?"));
	}
	
	@Test
	public void testDmlStatementGeneratorCasePersonGetEntiry(){
		String result = DmlStatementGenerator.getEntity(person());
		assertThat(result, is("select personId, firstName, familyName, age from person where personId=?"));
	}
	
	@Test
	public void testDmlStatementGeneratorCasePersonInsert(){
		String result = DmlStatementGenerator.insert(person());
		assertThat(result, is("insert into person (personId, firstName, familyName, age) values (?, ?, ?, ?)"));
	}
	
	@Test
	public void testDmlStatementGeneratorCasePersonUpdate(){
		String result = DmlStatementGenerator.update(person());
		assertThat(result, is("update person set firstName=?, familyName=?, age=? where personId=?"));
	}
	
	@Test
	public void testDmlStatementGeneratorCasePersonDelete(){
		String result = DmlStatementGenerator.delete(person());
		assertThat(result, is("delete from person where personId=?"));
	}

	static TableMeta account(){
		return TableMeta
				.table("account")
				.key("personId").type(ColumnType.LONG)
				.key("email").type(ColumnType.STRING)
				.attribute("firstName").type(ColumnType.STRING)
				.attribute("familyName").type(ColumnType.STRING)
				.attribute("age").type(ColumnType.LONG)
				.meta();
	}
	
	@Test
	public void testDMLStatementGeneratorCaseAccountSelectKey(){
		TableMeta account = account();
		String result = DmlStatementGenerator.selectKeys(account, "where personId=?");
		assertThat(result, is("select personId, email from account where personId=?"));
	}

	@Test
	public void testDMLStatementGeneratorCaseAccountSelectKey02(){
		TableMeta account = account();
		String result = DmlStatementGenerator.selectKeys(account, "where personId=? and email=?");
		assertThat(result, is("select personId, email from account where personId=? and email=?"));
	}
	
	@Test
	public void testDMLStatementGeneratorCaseAccountGetEntiry(){
		String result = DmlStatementGenerator.getEntity(account());
		assertThat(result, is("select personId, email, firstName, familyName, age from account where personId=? and email=?"));
	}
	
	@Test
	public void testDMLStatementGeneratorCaseAccountInsert(){
		String result = DmlStatementGenerator.insert(account());
		assertThat(result, is("insert into account (personId, email, firstName, familyName, age) values (?, ?, ?, ?, ?)"));
	}
	
	@Test
	public void testDMLStatementGeneratorCaseAccountUpdate(){
		String result = DmlStatementGenerator.update(account());
		assertThat(result, is("update account set firstName=?, familyName=?, age=? where personId=? and email=?"));
	}
	
	@Test
	public void testDMLStatementGeneratorCaseAccountDelete(){
		String result = DmlStatementGenerator.delete(account());
		assertThat(result, is("delete from account where personId=? and email=?"));
	}
}
