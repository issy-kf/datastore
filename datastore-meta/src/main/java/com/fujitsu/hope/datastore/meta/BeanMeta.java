package com.fujitsu.hope.datastore.meta;

public interface BeanMeta<BT>{
	public Class<BT> typeOf();
	public BT newInstance();
	public PropertyMeta<BT, ?> propertyMeta(String propertyName);
}