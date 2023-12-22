package com.fujitsu.hope.datastore.entities;

import com.fujitsu.hope.datastore.meta.AbstractEntityMeta;
import com.fujitsu.hope.datastore.meta.AttributeMeta;
import com.fujitsu.hope.datastore.meta.PropertyAccessor;

public class AccountMeta extends AbstractEntityMeta<Account>{
	private static final AccountMeta singleton = new AccountMeta(); 
	public static final AccountMeta get() { return singleton; }
	
	private final AttributeMeta.AttributeMetaBuilder<Account> builder = AttributeMeta.builder(singleton);

	public final AttributeMeta<Account, Long> personId;
	public final AttributeMeta<Account, String> email;
	public final AttributeMeta<Account, String> companyName;
	
	@SuppressWarnings("unchecked")
	public AccountMeta() {
		super(Account.class, "ACCOUNT");

		personId =
				builder.type(Long.class).name("personId", "PERSON_ID").
				accessor(new PropertyAccessor<Account, Long>() {
					@Override
					public Long get(Account bean) {
						return bean.getPersonId();
					}

					@Override
					public void set(Account bean, Long value) {
						bean.setPersonId(value);
					}
				});
		
		email =
				builder.type(String.class).name("email", "EMAIL").
				accessor(new PropertyAccessor<Account, String>() {
					@Override
					public String get(Account bean) {
						return bean.getEmail();
					}

					@Override
					public void set(Account bean, String value) {
						bean.setEmail(value);
					}
				});
		
		companyName = 
				builder.type(String.class).name("companyName", "COMPANY_NAME").
				accessor(new PropertyAccessor<Account, String>() {
					@Override
					public String get(Account bean) {
						return bean.getCompanyName();
					}

					@Override
					public void set(Account bean, String value) {
						bean.setCompanyName(value);
					}
				});
		
		defineKey(personId, email);
		defineAttribute(companyName);
	}

	
	@Override
	public Account newInstance() {
		return new Account();
	}
}
