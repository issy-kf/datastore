package com.fujitsu.hope.datastore;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fujitsu.hope.datastore.DataQueryTest.QueryFilter.QueryFilterLeaf;
import com.fujitsu.hope.datastore.DataQueryTest.QueryFilter.QueryFilterLeaf.QueryFilterOperator;
import com.fujitsu.hope.datastore.DataQueryTest.QueryFilter.QueryFilterComposite;
import com.fujitsu.hope.datastore.DataQueryTest.QueryFilter.QueryFilterComposite.QueryFilterCompositeType;
import com.fujitsu.hope.datastore.entities.Person;
import com.fujitsu.hope.datastore.entities.PersonMeta;
import com.fujitsu.hope.datastore.meta.AttributeMeta;
import com.fujitsu.hope.datastore.meta.EntityMeta;

public class DataQueryTest {
	private static Connection CONN;
	
	PersonMeta PERSON = PersonMeta.get();
	TableMeta T_PERSON = TableMeta.create(PERSON); 
	
	@BeforeClass
	public static void setUpClass() throws SQLException {
		CONN = SqliteUtils.get().createConnectionOnMemory();
	}
	
	@AfterClass
	public static void tearDownClass() throws SQLException {
		CONN.close();
	}
	
	@Before
	public void setUp() throws SQLException {
		SqliteUtils.get().ddl(CONN).create(T_PERSON);
	} 
	
	@After
	public void tearDown() throws SQLException {
		SqliteUtils.get().ddl(CONN).drop(T_PERSON);
	}

	static Person person(long personId, String familyName, String firstName, int age) {
		Person ret = new Person();
		ret.setPersonId(personId);
		ret.setFamilyName(familyName);
		ret.setFirstName(firstName);
		ret.setAge(age);
		return ret;
	}
	
	@Test
	public void testQuery001() {
		Query<Person> query = new Query<Person>(PERSON);
		query
			.filter(PERSON.firstName).equalsTo("hoge")
			.filter(PERSON.familyName).equalsTo("foo");
		String where = QueryInterpreter.whereStatement(query.filters);
		assertThat(where, is("where FIRST_NAME = ? and FAMILY_NAME = ?"));
	}
	
	@Test
	public void testQuery002() {
		Query<Person> query = new Query<Person>(PERSON);
		String where = QueryInterpreter.whereStatement(query.filters);
		assertThat(where, is("where FIRST_NAME = ? and FAMILY_NAME = ?"));
	}
	
	public class Query<E> {
		@SuppressWarnings("unused")
		private final EntityMeta<E> entityMeta;
		final List<QueryFilter<E>> filters;
		final List<QuerySorter<E>> sorters;
		
		Query(EntityMeta<E> entityMeta){
			this.entityMeta = entityMeta;
			this.filters = new ArrayList<QueryFilter<E>>();
			this.sorters = new ArrayList<QuerySorter<E>>();
		}

		public <A> QueryFilterBuilder<E, A> filter(AttributeMeta<E, A> attribute){
			return new QueryFilterBuilder<E, A>(this, attribute);
		}

		public <A> QuerySorterBuilder<E, A> sorter(AttributeMeta<E, A> attribute){
			return new QuerySorterBuilder<E, A>(this, attribute);
		}
		
		public class QueryFilterBuilder<ET, AT> {
			
			private final AttributeMeta<ET, AT> attributeMeta;
			
			private final Query<ET> parent;
			
			QueryFilterBuilder(Query<ET> parent, AttributeMeta<ET, AT> attributeMeta){
				 this.attributeMeta = attributeMeta;
				 this.parent = parent;
			}
			
			private Query<ET> addFilterToList(QueryFilterOperator operator, AT value) {
				parent.filters.add(new QueryFilterLeaf<ET, AT>(attributeMeta, operator, value));
				return parent;
			}
			
			public Query<ET> equalsTo(AT value){
				return addFilterToList(QueryFilterOperator.EQUAL, value);
			}
			public Query<ET> notEquals(AT value){
				return addFilterToList(QueryFilterOperator.NOT_EQUAL, value);
			}
			public Query<ET> greaterThan(AT value){
				return addFilterToList(QueryFilterOperator.GREATER_THAN, value);
			}
			public Query<ET> greaterEqual(AT value){
				return addFilterToList(QueryFilterOperator.GREATER_THAN_OR_EQUAL, value);
			}
			public Query<ET> lessThan(AT value){
				return addFilterToList(QueryFilterOperator.LESS_THAN, value);
			}
			public Query<ET> lessEqual(AT value){
				return addFilterToList(QueryFilterOperator.LESS_THAN_OR_EQUAL, value);
			}
			public Query<ET> startsWith(AT value){
				return addFilterToList(QueryFilterOperator.START_WITH, value);
			}
			public Query<ET> endsWith(AT value){
				return addFilterToList(QueryFilterOperator.END_WITH, value);
			}
			public Query<ET> contains(AT value){
				return addFilterToList(QueryFilterOperator.CONTAIN, value);
			}
		}
		
		class QuerySorterBuilder<ET,AT> {
			private final AttributeMeta<ET, AT> attributeMeta;
			private final Query<ET> parent;
			QuerySorterBuilder(Query<ET> parent, AttributeMeta<ET, AT> attributeMeta){
				this.attributeMeta = attributeMeta;
				this.parent = parent;
			}
			public Query<ET> asc(){
				this.parent.sorters.add(new QuerySorter<ET>(this.attributeMeta, QuerySorter.QuerySorterOperator.ASC));
				return this.parent;
			}
			public Query<ET> desc(){
				this.parent.sorters.add(new QuerySorter<ET>(this.attributeMeta, QuerySorter.QuerySorterOperator.DESC));
				return this.parent;
			}
		}
	}
	
	static class QueryInterpreter{
		@SuppressWarnings("unchecked")
		static <E> String whereStatement (List<QueryFilter<E>> filterList) {
			QueryFilter<E>[] array = new QueryFilter[filterList.size()];
			return whereStatement(filterList.toArray(array));
		}
		
		@SuppressWarnings("unchecked")
		static <E> String whereStatement (QueryFilter<E>... filters) {
			if (filters.length == 0) return "";
			else return "where "+toStatementForComposite("and", filters);
		}
		
		static <E> String toStatementForFilter(QueryFilter<E> filter) {
			if (filter instanceof QueryFilterComposite) return toStatementForComposite((QueryFilterComposite<E>)filter);
			else if (filter instanceof QueryFilterLeaf) return toStatementForFilterLeaf((QueryFilterLeaf<E, ?>)filter);
			else throw new IllegalArgumentException("illegal class Type:" + filter.getClass().getName());
		}
		
		static <E> String toStatementForComposite(QueryFilterComposite<E> composite) {
			if (composite.type == QueryFilterCompositeType.AND) 
				return toStatementForComposite("and", composite.criterias);
			else if (composite.type == QueryFilterCompositeType.OR) 
				return toStatementForComposite("or", composite.criterias);
			else 
				throw new IllegalArgumentException("illegal composite type:" + composite.type);
		}
		
		@SuppressWarnings("unchecked")
		static <E> String toStatementForComposite(String and_or, QueryFilter<E>... criterias) {
			StringBuilder sb = new StringBuilder();
			boolean first = true;
			for(QueryFilter<E> child : criterias){
				if(first) first = false;
				else sb.append(" "+and_or+" ");
				if(child instanceof QueryFilterLeaf)
					sb.append(toStatementForFilterLeaf((QueryFilterLeaf<E, ?>)child));
				else if (child instanceof QueryFilterComposite) 
					sb.append("("+toStatementForComposite((QueryFilterComposite<E>)child)+")");
				else throw new IllegalArgumentException(
						"interpreter can not resolve type:" + child.getClass().getName());
			}
			return sb.toString();
		}
		
		static <E,A> String toStatementForFilterLeaf(QueryFilterLeaf<E,A> filter) {
			return toStatementForFilterLeaf(filter.meta.columnName(), filter.operator);
		}
		
		static String toStatementForFilterLeaf(String columnName, QueryFilterOperator operator){
			if(operator == QueryFilterOperator.EQUAL)
				return columnName + " = ?";
			else if(operator == QueryFilterOperator.GREATER_THAN)
				return columnName + " > ?";
			else if(operator == QueryFilterOperator.GREATER_THAN_OR_EQUAL)
				return columnName + " >= ?";
			else if(operator == QueryFilterOperator.LESS_THAN)
				return columnName + " < ?";
			else if(operator == QueryFilterOperator.LESS_THAN_OR_EQUAL)
				return columnName + " <= ?";
			else if(operator == QueryFilterOperator.NOT_EQUAL)
				return columnName + " <> ?";
			else if(operator == QueryFilterOperator.START_WITH || 
					operator == QueryFilterOperator.END_WITH || 
					operator == QueryFilterOperator.CONTAIN)
				return columnName + " like ?";
			else throw new IllegalArgumentException("columnName=" + columnName + ", operator=" + operator);
		}
	}
	
	interface QueryFilter<E>{
		static class QueryFilterLeaf<E,A> implements QueryFilter<E>{
			static enum QueryFilterOperator	{
				EQUAL,
				NOT_EQUAL,
				LESS_THAN_OR_EQUAL,
				LESS_THAN,
				GREATER_THAN_OR_EQUAL,
				GREATER_THAN,
				START_WITH,
				END_WITH,
				CONTAIN
			}
			final AttributeMeta<E,A> meta;
			final A value;
			final QueryFilterOperator operator;

			QueryFilterLeaf(AttributeMeta<E,A> meta, QueryFilterOperator operator, A value) {
				this.meta = meta;
				this.value = value;
				this.operator = operator;
			}
		}
		
		public static class QueryFilterComposite<E> implements QueryFilter<E>{
			static enum QueryFilterCompositeType{
				AND, OR
			}
			final QueryFilterCompositeType type;
			final QueryFilter<E>[] criterias;
			@SuppressWarnings("unchecked")
			QueryFilterComposite(QueryFilterCompositeType type, QueryFilter<E>... criterias){
				this.type = type;
				this.criterias = criterias;
			}
			@SuppressWarnings("unchecked")
			public static <T> QueryFilterComposite<T> and(QueryFilter<T>... criterias){
				return new QueryFilterComposite<T>(QueryFilterCompositeType.AND, criterias);
			}
			@SuppressWarnings("unchecked")
			public static <T> QueryFilterComposite<T> or(QueryFilter<T>... criterias){
				return new QueryFilterComposite<T>(QueryFilterCompositeType.OR, criterias);
			}
		}
	}

	static class QuerySorter<E>{
		static enum QuerySorterOperator	{
			ASC,
			DESC,
		}
		final AttributeMeta<E,?> meta;
		final QuerySorterOperator operator;
		QuerySorter(AttributeMeta<E,?> meta, QuerySorterOperator operator) {
			this.meta = meta;
			this.operator = operator;
		}
	}
}