package com.fujitsu.hope.ds;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.fujitsu.hope.ds.TableMeta.ColumnMeta;

public class DmlStatementGeneratorTest {
	/**
	 * @return
	 */
	static TableMeta asAccount(){
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
	public void testSqlStatementHelperCaseInsert(){
		DmlStatementGenerator generator = new DmlStatementGenerator();
		String result = generator.insert(asAccount());
		assertThat(result, is("insert into account (personId, email, firstName, familyName, age) values (?, ?, ?, ?, ?)"));
	}
	
	@Test
	public void testSqlStatementHelperCaseUpdate(){
		DmlStatementGenerator generator = new DmlStatementGenerator();
		String result = generator.update(asAccount());
		assertThat(result, is("update account set firstName=?, familyName=?, age=? where personId=? and email=?"));
	}
	
	@Test
	public void testDMLStatementGeneratorCaseDelete(){
		DmlStatementGenerator generator = new DmlStatementGenerator();
		String result = generator.delete(asAccount());
		assertThat(result, is("delete from account where personId=? and email=?"));
	}
}
