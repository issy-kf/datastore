package com.fujitsu.hope.datastore.meta;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractEntityMeta<ET> extends AbstractBeanMeta<ET> implements EntityMeta<ET>{
	private final String tableName;
	private final List<AttributeMeta<ET,?>> keyList;
	private final List<AttributeMeta<ET,?>> attributeList;
	
	public AbstractEntityMeta(Class<ET> beanClass, String tableName) {
		super(beanClass);
		this.tableName = tableName;
		this.keyList = new ArrayList<AttributeMeta<ET, ?>>();
		this.attributeList = new ArrayList<AttributeMeta<ET, ?>>();
	}
	
	@SuppressWarnings("unchecked")
	protected void defineKey(AttributeMeta<ET, ?>... metas) {
		for(AttributeMeta<ET, ?> meta : metas) keyList.add(meta);
		defineProperties(metas);
	} 
	
	@SuppressWarnings("unchecked")
	protected void defineAttribute(AttributeMeta<ET, ?>... metas) {
		for(AttributeMeta<ET, ?> meta : metas) attributeList.add(meta);
		defineProperties(metas);
	} 
	
	@Override
	public String tableName() {
		return this.tableName;
	}

	@SuppressWarnings("unchecked")
	@Override
	public AttributeMeta<ET, ?>[] keys() {
		AttributeMeta<ET, ?>[] ret = new AttributeMeta[keyList.size()];
		keyList.toArray(ret);
		return ret;
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public AttributeMeta<ET, ?>[] attributes() {
		AttributeMeta<ET, ?>[] ret = new AttributeMeta[attributeList.size()];
		attributeList.toArray(ret);
		return ret; 
	}
}