package com.fujitsu.hope.ds;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.fujitsu.hope.ds.TableMeta.ColumnMeta;

public class DmlStatementGeneratorTest {
	static TableMeta person(){
		return TableMeta
				.table("person")
				.key("personId").type(ColumnMeta.LONG)
				.column("firstName").type(ColumnMeta.STRING)
				.column("familyName").type(ColumnMeta.STRING)
				.column("age").type(ColumnMeta.LONG)
				.meta();
	}

	@Test
	public void testDmlStatementGeneratorCasePersonSelectKey(){
		DmlStatementGenerator generator = new DmlStatementGenerator();
		String result = generator.selectKeys(person(), "where personId=?");
		assertThat(result, is("select personId from person where personId=?"));
	}
	
	@Test
	public void testDmlStatementGeneratorCasePersonGetEntiry(){
		DmlStatementGenerator generator = new DmlStatementGenerator();
		String result = generator.getEntity(person());
		assertThat(result, is("select personId, firstName, familyName, age from person where personId=?"));
	}
	
	@Test
	public void testDmlStatementGeneratorCasePersonInsert(){
		DmlStatementGenerator generator = new DmlStatementGenerator();
		String result = generator.insert(person());
		assertThat(result, is("insert into person (personId, firstName, familyName, age) values (?, ?, ?, ?)"));
	}
	
	@Test
	public void testDmlStatementGeneratorCasePersonUpdate(){
		DmlStatementGenerator generator = new DmlStatementGenerator();
		String result = generator.update(person());
		assertThat(result, is("update person set firstName=?, familyName=?, age=? where personId=?"));
	}
	
	@Test
	public void testDmlStatementGeneratorCasePersonDelete(){
		DmlStatementGenerator generator = new DmlStatementGenerator();
		String result = generator.delete(person());
		assertThat(result, is("delete from person where personId=?"));
	}

	static TableMeta account(){
		return TableMeta
				.table("account")
				.key("personId").type(ColumnMeta.LONG)
				.key("email").type(ColumnMeta.STRING)
				.column("firstName").type(ColumnMeta.STRING)
				.column("familyName").type(ColumnMeta.STRING)
				.column("age").type(ColumnMeta.LONG)
				.meta();
	}
	
	@Test
	public void testDmlStatementGeneratorCaseAccountSelectKey(){
		DmlStatementGenerator generator = new DmlStatementGenerator();
		TableMeta account = account();
		String result = generator.selectKeys(account, "where personId=?");
		assertThat(result, is("select personId, email from account where personId=?"));
	}

	@Test
	public void testDmlStatementGeneratorCaseAccountSelectKey02(){
		DmlStatementGenerator generator = new DmlStatementGenerator();
		TableMeta account = account();
		String result = generator.selectKeys(account, "where personId=? and email=?");
		assertThat(result, is("select personId, email from account where personId=? and email=?"));
	}
	
	@Test
	public void testDmlStatementGeneratorCaseAccountGetEntiry(){
		DmlStatementGenerator generator = new DmlStatementGenerator();
		String result = generator.getEntity(account());
		assertThat(result, is("select personId, email, firstName, familyName, age from account where personId=? and email=?"));
	}
	
	@Test
	public void testDMLStatementGeneratorCaseAccountInsert(){
		DmlStatementGenerator generator = new DmlStatementGenerator();
		String result = generator.insert(account());
		assertThat(result, is("insert into account (personId, email, firstName, familyName, age) values (?, ?, ?, ?, ?)"));
	}
	
	@Test
	public void testDMLStatementGeneratorCaseAccountUpdate(){
		DmlStatementGenerator generator = new DmlStatementGenerator();
		String result = generator.update(account());
		assertThat(result, is("update account set firstName=?, familyName=?, age=? where personId=? and email=?"));
	}
	
	@Test
	public void testDMLStatementGeneratorCaseAccountDelete(){
		DmlStatementGenerator generator = new DmlStatementGenerator();
		String result = generator.delete(account());
		assertThat(result, is("delete from account where personId=? and email=?"));
	}
}
