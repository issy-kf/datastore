package com.fujitsu.hope.datastore.meta;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.fujitsu.hope.datastore.entities.Account;
import com.fujitsu.hope.datastore.entities.AccountMeta;
import com.fujitsu.hope.datastore.entities.Person;
import com.fujitsu.hope.datastore.entities.PersonMeta;




public class EntityMetaTest {
	private static PersonMeta PERSON = PersonMeta.get();
	private static AccountMeta ACCOUNT = AccountMeta.get();
	
	@Test
	public void testPersonMeta() {
		AttributeMeta<Person, ?>[] keys = PERSON.keys();
		assertThat(keys.length, is(1));
	}
	
	@Test
	public void testPersonMeta000() {
		assertThat(PERSON.typeOf().getName(), is(Person.class.getName()));
		assertThat(PERSON.tableName(), is("PERSON"));
		assertThat(PERSON.personId.columnName(), is("PERSON_ID"));
		assertThat(PERSON.familyName.columnName(), is("FAMILY_NAME"));
		assertThat(PERSON.firstName.columnName(), is("FIRST_NAME"));
		assertThat(PERSON.age.columnName(), is("AGE"));
		assertThat(PERSON.keys().length, is(1));
		assertThat(PERSON.attributes().length, is(3));
	}
	
	
	@Test
	public void testAccountMeta000() {
		assertThat(ACCOUNT.typeOf().getName(), is(Account.class.getName()));
		assertThat(ACCOUNT.tableName(), is("ACCOUNT"));
		assertThat(ACCOUNT.personId.columnName(), is("PERSON_ID"));
		assertThat(ACCOUNT.email.columnName(), is("EMAIL"));
		assertThat(ACCOUNT.companyName.columnName(), is("COMPANY_NAME"));
	}
	
	@Test
	public void testPersonMeta001() {
		Person p0 = PERSON.newInstance();
		p0.setPersonId(101L);
		p0.setFamilyName("foo");
		p0.setFirstName("bar");
		p0.setAge(35);
		
		assertThat(PERSON.familyName.get(p0), is("foo"));
		PERSON.familyName.set(p0, "foo1");
		assertThat(p0.getFamilyName(), is("foo1"));
		assertThat(p0.getAge(), is(35));
		PERSON.age.set(p0, 38);
		assertThat(p0.getAge(), is(38));
	}

	@Test
	public void testAccountMeta001() {
		Account a0 = ACCOUNT.newInstance();
		a0.setPersonId(201L);
		a0.setEmail("foo@bar.com");
		a0.setCompanyName("bar");
		assertThat(ACCOUNT.email.get(a0), is("foo@bar.com"));
		ACCOUNT.email.set(a0, "doo@bar.com");
		assertThat(a0.getEmail(), is("doo@bar.com"));
		assertThat(a0.getCompanyName(), is("bar"));
		ACCOUNT.companyName.set(a0, "bar2");
		assertThat(a0.getCompanyName(), is("bar2"));
	}
}
