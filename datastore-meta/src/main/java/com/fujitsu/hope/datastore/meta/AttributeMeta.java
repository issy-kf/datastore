package com.fujitsu.hope.datastore.meta;

import java.math.BigDecimal;

public class AttributeMeta<ET,CT> extends PropertyMeta<ET,CT>{
	private final String columnName;
	private final ColumnType columnType;

	AttributeMeta(BeanMeta<ET> beanMeta, Class<CT> propertyType, String propertyName, String columnName, ColumnType columnType, PropertyAccessor<ET, CT> resolver) {
		super(beanMeta, propertyType, propertyName, resolver);
		this.columnName = columnName;
		this.columnType = columnType;
	}

	public String columnName() {
		return this.columnName;
	}
	
	public ColumnType columnType() {
		return this.columnType;
	}
	
	public static <ET> AttributeMetaBuilder<ET> builder(EntityMeta<ET> entityMeta){
		return new AttributeMetaBuilder<ET>(entityMeta);
	}

	public static class AttributeMetaBuilder<E>{
		private final EntityMeta<E> entityMeta;
		AttributeMetaBuilder(EntityMeta<E> entityMeta){
			this.entityMeta = entityMeta;
		}
		public <C> AttributeTypeOf<E,C> type(Class<C> propertyType){
			return new AttributeTypeOf<E,C>(entityMeta, propertyType, toColumnType(propertyType));
		} 
		public <C> AttributeTypeOf<E,C> type(Class<C> propertyType, ColumnType columnType){
			return new AttributeTypeOf<E,C>(entityMeta, propertyType, columnType);
		} 
	}

	static <T> ColumnType toColumnType(Class<T> cls) {
		if (cls.equals(String.class)) return ColumnType.STRING;
		else if (cls.equals(Integer.class)) return ColumnType.INTEGER;
		else if (cls.equals(Long.class)) return ColumnType.LONG;
		else if (cls.equals(Byte.class)) return ColumnType.BYTE;
		else if (cls.equals(Short.class)) return ColumnType.SHORT;
		else if (cls.equals(Float.class)) return ColumnType.FLOAT;
		else if (cls.equals(Double.class)) return ColumnType.DOUBLE;
		else if (cls.equals(BigDecimal.class)) return ColumnType.BIG_DECIMAL;
		else if (cls.equals(Boolean.class)) return ColumnType.BOOLEAN;
		else throw new IllegalArgumentException(cls.getName()+" is not supported");
	}
	
	public static class AttributeTypeOf<E,C> {
		private final EntityMeta<E> entityMeta;
		private final Class<C> propertyType;
		private final ColumnType columnType;
		AttributeTypeOf(EntityMeta<E> entityMeta, Class<C> propertyType, ColumnType columnType){
			this.entityMeta = entityMeta;
			this.propertyType = propertyType;
			this.columnType = columnType;
		}

		public AttributeNameOf<E,C> name(String propertyName, String columnName){
			return new AttributeNameOf<E,C>(
					entityMeta,
					propertyType,
					propertyName,
					columnName,
					columnType);
		}
		
		public AttributeNameOf<E,C> name(String propertyName){
			return new AttributeNameOf<E,C>(
					entityMeta,
					propertyType,
					propertyName,
					propertyName,
					columnType);
		}
	}

	public static class AttributeNameOf<E,C>{
		private final EntityMeta<E> entityMeta;
		private final Class<C> propertyType;
		private final ColumnType columnType;
		private final String propertyName;
		private final String columnName;

		AttributeNameOf(EntityMeta<E> entityMeta,
				Class<C> propertyType,
				String propertyName,
				String columnName,
				ColumnType columnType){
			this.entityMeta = entityMeta;
			this.propertyType = propertyType;
			this.propertyName = propertyName;
			this.columnName = columnName;
			this.columnType = columnType;
		}

		public AttributeMeta<E,C> accessor(PropertyAccessor<E,C> accessor){
			return new AttributeMeta<E,C>(
					entityMeta,
					propertyType,
					propertyName,
					columnName,
					columnType,
					accessor);
		}
	}
}