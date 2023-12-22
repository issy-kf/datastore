package com.fujitsu.hope.datastore.beans;

import com.fujitsu.hope.datastore.meta.AbstractBeanMeta;
import com.fujitsu.hope.datastore.meta.PropertyAccessor;
import com.fujitsu.hope.datastore.meta.PropertyMeta;

public class AccountMeta extends AbstractBeanMeta<Account>{
	private static final AccountMeta singleton = new AccountMeta();
	public static final AccountMeta get() { return singleton; }
	
	public final PropertyMeta<Account, Long> personId;
	public final PropertyMeta<Account, String> email;
	public final PropertyMeta<Account, String> companyName;

	@SuppressWarnings("unchecked")
	private AccountMeta() { 
		super(Account.class);
		PropertyMeta.PropertyMetaBuilder<Account> builder = 
				PropertyMeta.builder(singleton);
		
		this.personId = 
				builder.type(Long.class).name("personId").
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
		
		this.email = 
				builder.type(String.class).name("email").
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
		
		this.companyName =
				builder.type(String.class).name("companyName").
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
		
		defineProperties(personId, email, companyName);
	}
	
	@Override 
	public Account newInstance() { 
		return new Account();
	}
}
