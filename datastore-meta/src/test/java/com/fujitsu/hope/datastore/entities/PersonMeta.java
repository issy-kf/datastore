package com.fujitsu.hope.datastore.entities;

import com.fujitsu.hope.datastore.meta.AbstractEntityMeta;
import com.fujitsu.hope.datastore.meta.AttributeMeta;
import com.fujitsu.hope.datastore.meta.PropertyAccessor;

public class PersonMeta extends AbstractEntityMeta<Person>{
	private static final PersonMeta singleton = new PersonMeta(); 
	public static final PersonMeta get() { return singleton; }

	private final AttributeMeta.AttributeMetaBuilder<Person> builder = AttributeMeta.builder(singleton);
	
	public final AttributeMeta<Person, Long> personId;
	public final AttributeMeta<Person, String> familyName;
	public final AttributeMeta<Person, String> firstName;
	public final AttributeMeta<Person, Integer> age;
	
	@SuppressWarnings("unchecked")
	public PersonMeta() {
		super(Person.class, "PERSON");

		personId =
				builder.type(Long.class).name("personId", "PERSON_ID").
				accessor(new PropertyAccessor<Person, Long>() {
					@Override
					public Long get(Person bean) {
						return bean.getPersonId();
					}

					@Override
					public void set(Person bean, Long value) {
						bean.setPersonId(value);
					}
				});
		
		familyName =
				builder.type(String.class).name("familyName", "FAMILY_NAME").
				accessor(new PropertyAccessor<Person, String>() {
					@Override
					public String get(Person bean) {
						return bean.getFamilyName();
					}

					@Override
					public void set(Person bean, String value) {
						bean.setFamilyName(value);
					}
				});
		
		firstName = 
				builder.type(String.class).name("firstName", "FIRST_NAME").
				accessor(new PropertyAccessor<Person, String>() {
					@Override
					public String get(Person bean) {
						return bean.getFirstName();
					}

					@Override
					public void set(Person bean, String value) {
						bean.setFirstName(value);
					}
				});

		age = 
				builder.type(Integer.class).name("age", "AGE").
				accessor(new PropertyAccessor<Person, Integer>() {
					@Override
					public Integer get(Person bean) {
						return bean.getAge();
					}

					@Override
					public void set(Person bean, Integer value) {
						bean.setAge(value);
					}
				});
		
		defineKey(personId);
		defineAttribute(familyName, firstName, age);
	}
	
	@Override
	public Person newInstance() {
		return new Person();
	}
}
