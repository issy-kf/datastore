package com.fujitsu.hope.datastore;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fujitsu.hope.datastore.DataShelf.UpdateResult;
import com.fujitsu.hope.datastore.entities.Person;
import com.fujitsu.hope.datastore.entities.PersonMeta;
import com.fujitsu.hope.datastore.meta.Key;


public class DetaShelfTest {
	private static Connection CONN;
	
	PersonMeta PERSON = PersonMeta.get();
	TableMeta T_PERSON = TableMeta.create(PERSON); 
	
	@BeforeClass
	public static void setUpClass() throws SQLException {
		CONN = SqliteUtils.get().createConnectionOnMemory();
	}
	
	@AfterClass
	public static void tearDownClass() throws SQLException {
		CONN.close();
	}
	
	@Before
	public void setUp() throws SQLException {
		SqliteUtils.get().ddl(CONN).create(T_PERSON);
	} 
	
	@After
	public void tearDown() throws SQLException {
		SqliteUtils.get().ddl(CONN).drop(T_PERSON);
	}

	static Person person(long personId, String familyName, String firstName, int age) {
		Person ret = new Person();
		ret.setPersonId(personId);
		ret.setFamilyName(familyName);
		ret.setFirstName(firstName);
		ret.setAge(age);
		return ret;
	}
	
	@Test
	public void tableDataShelf001() throws InterruptedException, ExecutionException {
		Datastore store = new Datastore("test_service", CONN);
		DataShelf<Person> shelf = store.shelf(PERSON);
		Person p101 = person(101L,"101","bar",11);
		Person p102 = person(102L,"102","bar",12);
		Person p103 = person(103L,"103","bar",13);
		Person p104 = person(104L,"104","bar",14);
		Person p105 = person(105L,"105","bar",15);
		Person p106 = person(106L,"106","bar",16);
		
		assertThat(shelf.insert(p101).get(),is(UpdateResult.SUCCESS));
		assertThat(shelf.insert(p102).get(),is(UpdateResult.SUCCESS));
		assertThat(shelf.insert(p103).get(),is(UpdateResult.SUCCESS));
		assertThat(shelf.insert(p104).get(),is(UpdateResult.SUCCESS));
		assertThat(shelf.insert(p105).get(),is(UpdateResult.SUCCESS));
		assertThat(shelf.insert(p106).get(),is(UpdateResult.SUCCESS));

		Person p101_ = shelf.entityOf(Key.builder(PERSON).type(PERSON.personId).value(101l).build()).get();
		Person p102_ = shelf.entityOf(Key.builder(PERSON).type(PERSON.personId).value(102l).build()).get();
		Person p103_ = shelf.entityOf(Key.builder(PERSON).type(PERSON.personId).value(103l).build()).get();
		Person p104_ = shelf.entityOf(Key.builder(PERSON).type(PERSON.personId).value(104l).build()).get();
		Person p105_ = shelf.entityOf(Key.builder(PERSON).type(PERSON.personId).value(105l).build()).get();
		Person p106_ = shelf.entityOf(Key.builder(PERSON).type(PERSON.personId).value(106l).build()).get();

		assertThat(p101_.getPersonId(), is(p101.getPersonId()));
		assertThat(p101_.getFamilyName(), is(p101.getFamilyName()));
		assertThat(p101_.getFirstName(), is(p101.getFirstName()));
		assertThat(p101_.getAge(), is(p101.getAge()));
		assertThat(p101.equals(p101_), is(false));

		assertThat(p102_.getPersonId(), is(p102.getPersonId()));
		assertThat(p102_.getFamilyName(), is(p102.getFamilyName()));
		assertThat(p102_.getFirstName(), is(p102.getFirstName()));
		assertThat(p102_.getAge(), is(p102.getAge()));
		assertThat(p102.equals(p102_), is(false));
		
		assertThat(p103_.getPersonId(), is(p103.getPersonId()));
		assertThat(p103_.getFamilyName(), is(p103.getFamilyName()));
		assertThat(p103_.getFirstName(), is(p103.getFirstName()));
		assertThat(p103_.getAge(), is(p103.getAge()));
		assertThat(p103.equals(p103_), is(false));

		assertThat(p104_.getPersonId(), is(p104.getPersonId()));
		assertThat(p104_.getFamilyName(), is(p104.getFamilyName()));
		assertThat(p104_.getFirstName(), is(p104.getFirstName()));
		assertThat(p104_.getAge(), is(p104.getAge()));
		assertThat(p104.equals(p104_), is(false));
		
		assertThat(p105_.getPersonId(), is(p105.getPersonId()));
		assertThat(p105_.getFamilyName(), is(p105.getFamilyName()));
		assertThat(p105_.getFirstName(), is(p105.getFirstName()));
		assertThat(p105_.getAge(), is(p105.getAge()));
		assertThat(p105.equals(p105_), is(false));

		assertThat(p106_.getPersonId(), is(p106.getPersonId()));
		assertThat(p106_.getFamilyName(), is(p106.getFamilyName()));
		assertThat(p106_.getFirstName(), is(p106.getFirstName()));
		assertThat(p106_.getAge(), is(p106.getAge()));
		assertThat(p106.equals(p105_), is(false));

		p101.setFirstName("foo");
		p102.setFirstName("foo");
		p103.setFirstName("foo");
		p104.setFirstName("foo");
		p105.setFirstName("foo");
		p106.setFirstName("foo");

		p101.setAge(21);
		p102.setAge(22);
		p103.setAge(23);
		p104.setAge(24);
		p105.setAge(25);
		p106.setAge(26);

		assertThat(shelf.update(p101).get(),is(UpdateResult.SUCCESS));
		assertThat(shelf.update(p102).get(),is(UpdateResult.SUCCESS));
		assertThat(shelf.update(p103).get(),is(UpdateResult.SUCCESS));
		assertThat(shelf.update(p104).get(),is(UpdateResult.SUCCESS));
		assertThat(shelf.update(p105).get(),is(UpdateResult.SUCCESS));
		assertThat(shelf.update(p106).get(),is(UpdateResult.SUCCESS));
		
		Person p101__ = shelf.entityOf(Key.builder(PERSON).type(PERSON.personId).value(101l).build()).get();
		Person p102__ = shelf.entityOf(Key.builder(PERSON).type(PERSON.personId).value(102l).build()).get();
		Person p103__ = shelf.entityOf(Key.builder(PERSON).type(PERSON.personId).value(103l).build()).get();
		Person p104__ = shelf.entityOf(Key.builder(PERSON).type(PERSON.personId).value(104l).build()).get();
		Person p105__ = shelf.entityOf(Key.builder(PERSON).type(PERSON.personId).value(105l).build()).get();
		Person p106__ = shelf.entityOf(Key.builder(PERSON).type(PERSON.personId).value(106l).build()).get();
		
		assertThat(p101__.getPersonId(), is(p101.getPersonId()));
		assertThat(p101__.getFamilyName(), is(p101.getFamilyName()));
		assertThat(p101__.getFirstName(), is(p101.getFirstName()));
		assertThat(p101__.getAge(), is(p101.getAge()));
		assertThat(p101__.equals(p101), is(false));

		assertThat(p102__.getPersonId(), is(p102.getPersonId()));
		assertThat(p102__.getFamilyName(), is(p102.getFamilyName()));
		assertThat(p102__.getFirstName(), is(p102.getFirstName()));
		assertThat(p102__.getAge(), is(p102.getAge()));
		assertThat(p102__.equals(p102), is(false));
		
		assertThat(p103__.getPersonId(), is(p103.getPersonId()));
		assertThat(p103__.getFamilyName(), is(p103.getFamilyName()));
		assertThat(p103__.getFirstName(), is(p103.getFirstName()));
		assertThat(p103__.getAge(), is(p103.getAge()));
		assertThat(p103__.equals(p103), is(false));

		assertThat(p104__.getPersonId(), is(p104.getPersonId()));
		assertThat(p104__.getFamilyName(), is(p104.getFamilyName()));
		assertThat(p104__.getFirstName(), is(p104.getFirstName()));
		assertThat(p104__.getAge(), is(p104.getAge()));
		assertThat(p104__.equals(p104), is(false));
		
		assertThat(p105__.getPersonId(), is(p105.getPersonId()));
		assertThat(p105__.getFamilyName(), is(p105.getFamilyName()));
		assertThat(p105__.getFirstName(), is(p105.getFirstName()));
		assertThat(p105__.getAge(), is(p105.getAge()));
		assertThat(p105__.equals(p105), is(false));

		assertThat(p106__.getPersonId(), is(p106.getPersonId()));
		assertThat(p106__.getFamilyName(), is(p106.getFamilyName()));
		assertThat(p106__.getFirstName(), is(p106.getFirstName()));
		assertThat(p106__.getAge(), is(p106.getAge()));
		assertThat(p106__.equals(p105), is(false));
		
		assertThat(shelf.delete(Key.builder(PERSON).type(PERSON.personId).value(101l).build()).get(),is(UpdateResult.SUCCESS));
		assertThat(shelf.delete(Key.builder(PERSON).type(PERSON.personId).value(102l).build()).get(),is(UpdateResult.SUCCESS));
		assertThat(shelf.delete(Key.builder(PERSON).type(PERSON.personId).value(103l).build()).get(),is(UpdateResult.SUCCESS));
		assertThat(shelf.delete(Key.builder(PERSON).type(PERSON.personId).value(104l).build()).get(),is(UpdateResult.SUCCESS));
		assertThat(shelf.delete(Key.builder(PERSON).type(PERSON.personId).value(105l).build()).get(),is(UpdateResult.SUCCESS));
		assertThat(shelf.delete(Key.builder(PERSON).type(PERSON.personId).value(106l).build()).get(),is(UpdateResult.SUCCESS));

		assertThat(shelf.delete(Key.builder(PERSON).type(PERSON.personId).value(101l).build()).get(),is(UpdateResult.NOT_EXITED));
		assertThat(shelf.delete(Key.builder(PERSON).type(PERSON.personId).value(102l).build()).get(),is(UpdateResult.NOT_EXITED));
		assertThat(shelf.delete(Key.builder(PERSON).type(PERSON.personId).value(103l).build()).get(),is(UpdateResult.NOT_EXITED));
		assertThat(shelf.delete(Key.builder(PERSON).type(PERSON.personId).value(104l).build()).get(),is(UpdateResult.NOT_EXITED));
		assertThat(shelf.delete(Key.builder(PERSON).type(PERSON.personId).value(105l).build()).get(),is(UpdateResult.NOT_EXITED));
		assertThat(shelf.delete(Key.builder(PERSON).type(PERSON.personId).value(106l).build()).get(),is(UpdateResult.NOT_EXITED));

		assertThat(shelf.entityOf(Key.builder(PERSON).type(PERSON.personId).value(101l).build()).get() == null, is(true));
		assertThat(shelf.entityOf(Key.builder(PERSON).type(PERSON.personId).value(102l).build()).get() == null, is(true));
		assertThat(shelf.entityOf(Key.builder(PERSON).type(PERSON.personId).value(103l).build()).get() == null, is(true));
		assertThat(shelf.entityOf(Key.builder(PERSON).type(PERSON.personId).value(104l).build()).get() == null, is(true));
		assertThat(shelf.entityOf(Key.builder(PERSON).type(PERSON.personId).value(105l).build()).get() == null, is(true));
		assertThat(shelf.entityOf(Key.builder(PERSON).type(PERSON.personId).value(106l).build()).get() == null, is(true));
	}
}