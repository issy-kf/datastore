package com.fujitsu.hope.datastore.meta;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractBeanMeta<BT> implements BeanMeta<BT>{
	private final Class<BT> beanClass;
	private final Map<String, PropertyMeta<BT, ?>> propertyMap;
	public AbstractBeanMeta(Class<BT> beanClass){
		this.beanClass = beanClass;
		this.propertyMap = new HashMap<String, PropertyMeta<BT,?>>();
	}
	
	@Override
	public Class<BT> typeOf(){
		return this.beanClass;
	}
	
	@Override
	public PropertyMeta<BT, ?> propertyMeta(String propertyName) {
		return propertyMap.get(propertyName);
	}
	
	@SuppressWarnings("unchecked")
	protected void defineProperties(PropertyMeta<BT, ?>... metas) {
		for (PropertyMeta<BT, ?> meta : metas) 
			propertyMap.put(meta.propertyName(), meta);
	}
}