package com.fujitsu.hope.datastore.meta;

public class PropertyMeta<BT, PT>{
	private final BeanMeta<BT> beanMeta;
	private final String propertyName;
	private final PropertyAccessor<BT,PT> accessor;
	private final Class<PT> propertyType;

	PropertyMeta(BeanMeta<BT> beanMeta, Class<PT> propertyType, String propertyName, PropertyAccessor<BT,PT> resolver){
		this.beanMeta = beanMeta;
		this.propertyName = propertyName;
		this.accessor = resolver;
		this.propertyType = propertyType;
	}

	public BeanMeta<BT> beanMeta(){
		return this.beanMeta;
	}

	public String propertyName() { 
		return this.propertyName; 
	}

	public Class<PT> propertyType() { 
		return this.propertyType; 
	}

	public PT get(BT bean){
		return this.accessor.get(bean);
	}

	public void set(BT bean, PT value){
		this.accessor.set(bean, value);
	}

	public static <BT> PropertyMetaBuilder<BT> builder(BeanMeta<BT> beanMeta){
		return new PropertyMetaBuilder<BT>(beanMeta);
	}
	
	public static class PropertyMetaBuilder<B> {
		private final BeanMeta<B> beanMeta;

		PropertyMetaBuilder(BeanMeta<B> beanMeta){
			this.beanMeta = beanMeta;
		}

		public <P> PropertyTypeOf<B, P> type(Class<P> type){
			return new PropertyTypeOf<B, P>(this.beanMeta, type); 
		}
	}
	
	public static class PropertyTypeOf<B,P> {
		private final BeanMeta<B> beanMeta;
		private final Class<P> propertyType;

		PropertyTypeOf(BeanMeta<B> beanMeta, Class<P> propertyType){
			this.beanMeta = beanMeta;
			this.propertyType = propertyType;
		}
		
		public PropertyNameOf<B,P> name(String propertyName){
			return new PropertyNameOf<B, P>(beanMeta, propertyType, propertyName);
		}
	} 
	
	public static class PropertyNameOf<B, P>{
		private final BeanMeta<B> beanMeta;
		private final Class<P> propertyType;
		private final String propertyName;

		PropertyNameOf(BeanMeta<B> beanMeta, Class<P> propertyType, String propertyName){
			this.beanMeta = beanMeta;
			this.propertyType = propertyType;
			this.propertyName = propertyName;
		}

		public PropertyMeta<B,P> accessor(PropertyAccessor<B,P> accessor){
			return new PropertyMeta<B,P>(beanMeta, propertyType, propertyName, accessor);
		}
	}
}