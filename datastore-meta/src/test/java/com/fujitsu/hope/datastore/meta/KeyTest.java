package com.fujitsu.hope.datastore.meta;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.fujitsu.hope.datastore.entities.Account;
import com.fujitsu.hope.datastore.entities.AccountMeta;
import com.fujitsu.hope.datastore.entities.Person;
import com.fujitsu.hope.datastore.entities.PersonMeta;

public class KeyTest {
	private static PersonMeta PERSON = PersonMeta.get();
	private static AccountMeta ACCOUNT = AccountMeta.get();

	@Test
	public void testKey0000() {
		@SuppressWarnings({ "unchecked", "rawtypes" })
		Key<Person> key = new Key(PERSON, 101l);
		assertThat(key.values().length, is(1));
		assertThat(key.values()[0], is(101l));
	}

	@Test
	public void testKey0001() {
		@SuppressWarnings({ "unchecked", "rawtypes" })
		Key<Account> key = new Key(ACCOUNT, 101l, "hoge@foo.com");
		assertThat(key.values().length, is(2));
		assertThat(key.values()[0], is(101l));
		assertThat(key.values()[1], is("hoge@foo.com"));
	}

	@Test
	public void testKey0002() {
		Key<Person> key = Key.builder(PERSON).type(PERSON.personId).value(101L).build();
		assertThat(key.values().length, is(1));
		assertThat(key.values()[0], is(101l));
	}

	@Test
	public void testKey0003() {
		Key<Account> key = Key.builder(ACCOUNT)
				.type(ACCOUNT.personId).value(101L)
				.type(ACCOUNT.email).value("hoge@foo.com")
				.build();
		assertThat(key.values().length, is(2));
		assertThat(key.values()[0], is(101l));
		assertThat(key.values()[1], is("hoge@foo.com"));
	}
}
