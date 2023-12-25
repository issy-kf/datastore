package com.fujitsu.hope.datastore.meta;

public interface PropertyAccessor<BT,PT>{
	public PT get(BT bean);
	public void set(BT bean, PT value);
}