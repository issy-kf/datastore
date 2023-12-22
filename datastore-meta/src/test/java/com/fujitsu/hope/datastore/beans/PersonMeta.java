package com.fujitsu.hope.datastore.beans;

import com.fujitsu.hope.datastore.meta.AbstractBeanMeta;
import com.fujitsu.hope.datastore.meta.PropertyAccessor;
import com.fujitsu.hope.datastore.meta.PropertyMeta;

public class PersonMeta extends AbstractBeanMeta<Person> {
	private final static PersonMeta singleton = new PersonMeta();
	public static PersonMeta get() { return singleton; }

	public final PropertyMeta<Person, Long> personId;
	public final PropertyMeta<Person, String> familyName;
	public final PropertyMeta<Person, String> firstName;
	public final PropertyMeta<Person, Integer> age;

	@SuppressWarnings("unchecked")
	private PersonMeta() {
		super(Person.class);
		PropertyMeta.PropertyMetaBuilder<Person> builder = 
				PropertyMeta.builder(singleton);
		this.personId = 
				builder.type(Long.class).name("personId").
				accessor(new PropertyAccessor<Person, Long>() {
					@Override 
					public Long get(Person bean) { 
						return bean.getPersonId(); }
					@Override
					public void set(Person bean, Long value) { 
						bean.setPersonId(value); }
					});
		this.familyName = 
				builder.type(String.class).name("familyName").
				accessor(new PropertyAccessor<Person, String>() {
					@Override 
					public String get(Person bean) { 
						return bean.getFamilyName(); 
						}
					@Override 
					public void set(Person bean, String value) { 
						bean.setFamilyName(value); }
					});
		this.firstName = 
				builder.type(String.class).name("firstName").
				accessor(new PropertyAccessor<Person, String>() {
					@Override 
					public String get(Person bean) { 
						return bean.getFirstName(); }
					@Override 
					public void set(Person bean, String value) { 
						bean.setFirstName(value); }
					});
		this.age = 
				builder.type(Integer.class).name("age").
				accessor(new PropertyAccessor<Person, Integer>() {
					@Override 
					public Integer get(Person bean) { 
						return bean.getAge(); }
					@Override 
					public void set(Person bean, Integer value) { 
						bean.setAge(value); }
					});
		this.defineProperties(personId, familyName, firstName, age);
	}
	@Override 
	public Person newInstance() { 
		return new Person();
		}
}