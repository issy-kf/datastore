package com.fujitsu.hope.datastore.meta;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.fujitsu.hope.datastore.beans.Account;
import com.fujitsu.hope.datastore.beans.AccountMeta;
import com.fujitsu.hope.datastore.beans.Person;
import com.fujitsu.hope.datastore.beans.PersonMeta;

public class BeanMetaTest {
	private static PersonMeta PERSON = PersonMeta.get();
	private static AccountMeta ACCOUNT = AccountMeta.get();

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
	
	@SuppressWarnings({ "unchecked", "static-access" })
	@Test
	public void testPersonMeta002() {
		PropertyMeta<Person, String> familyName = (PropertyMeta<Person, String>) PERSON.get().propertyMeta("familyName");
		assertThat(familyName.propertyName(), is("familyName"));
		assertThat(familyName.propertyType(), is(String.class.getClass()));
		PropertyMeta<Person, Integer> age = (PropertyMeta<Person, Integer>) PERSON.get().propertyMeta("age");
		assertThat(age.propertyName(), is("age"));
		assertThat(age.propertyType(), is(Integer.class.getClass()));
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

	@SuppressWarnings({ "unchecked", "static-access" })
	@Test
	public void testAccountMeta002() {
		PropertyMeta<Account, String> email = (PropertyMeta<Account, String>) ACCOUNT.get().propertyMeta("email");
		assertThat(email.propertyName(), is("email"));
		assertThat(email.propertyType(), is(String.class.getClass()));
		PropertyMeta<Account, String> companyName = (PropertyMeta<Account, String>) ACCOUNT.get().propertyMeta("companyName");
		assertThat(companyName.propertyName(), is("companyName"));
		assertThat(companyName.propertyType(), is(String.class.getClass()));
	}
}