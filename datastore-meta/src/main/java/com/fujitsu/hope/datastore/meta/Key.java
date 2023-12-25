package com.fujitsu.hope.datastore.meta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Key<ET> {
	private final Object[] values;
	private final EntityMeta<ET> meta;

	Key(EntityMeta<ET> meta, Object... values) {
		this.meta = meta;
		this.values = values;
	}

	public EntityMeta<ET> meta(){ return this.meta; }
	
	public Object[] values() { return values; }
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		@SuppressWarnings("unchecked")
		Key<ET> other = (Key<ET>) obj;
		if (!this.meta.equals(other.meta())) return false;
		if (!Arrays.equals(values, other.values)) return false;
		return true;
	}
	
	public static final <E> KeyBuilder<E> builder(EntityMeta<E> meta) {
		return new KeyBuilder<E>(meta);
	}
	
	public static class KeyBuilder<E> {
		private final EntityMeta<E> entityMeta;
		private final List<Object> values;
		private final List<AttributeMeta<E, ?>> attrs;

		KeyBuilder(EntityMeta<E> entityMeta){
			this.entityMeta = entityMeta;
			this.values = new ArrayList<Object>();			
			this.attrs = new ArrayList<AttributeMeta<E, ?>>();			
		}

		public <C> KeyTypeOf<E, C> type(AttributeMeta<E, C> attr){
			return new KeyTypeOf<E, C>(this, attr);
		}

		List<Object> values(){ return this.values; }

		List<AttributeMeta<E, ?>> attributes(){ return this.attrs; }

		public Key<E> build(){
			if (values.size() != entityMeta.keys().length) 
				throw new IllegalArgumentException("key values length["+values.size()+"] is illegal.");
			for(int i=0; i<entityMeta.keys().length; i++) 
				if (!entityMeta.keys()[i].propertyType().equals(attrs.get(i).propertyType()))
					throw new IllegalArgumentException("type of key["+i+"] value is illegal.["+attrs.get(i).propertyType()+"] is illegal.");
			return new Key<E>(entityMeta, values.toArray());
		}
	}
	
	public static class KeyTypeOf<E, C>{
		private final AttributeMeta<E, C> attributeMeta;
		private final KeyBuilder<E> keyBuilder;
		KeyTypeOf(KeyBuilder<E> keyBuilder, AttributeMeta<E, C> attributeMeta){
			this.attributeMeta = attributeMeta;
			this.keyBuilder = keyBuilder;
		}
		public KeyBuilder<E> value(C value){
			this.keyBuilder.values().add(value);
			this.keyBuilder.attributes().add(this.attributeMeta);
			return this.keyBuilder;
		}
	}
}
