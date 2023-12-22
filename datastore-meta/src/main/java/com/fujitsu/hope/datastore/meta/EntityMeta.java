package com.fujitsu.hope.datastore.meta;

public interface EntityMeta <ET> extends BeanMeta <ET>{
	public String tableName();
	public AttributeMeta<ET, ?>[] keys();
	public AttributeMeta<ET, ?>[] attributes();
}
