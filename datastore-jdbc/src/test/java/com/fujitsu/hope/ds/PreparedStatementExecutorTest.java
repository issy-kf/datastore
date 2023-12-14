package com.fujitsu.hope.ds;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fujitsu.hope.ds.TableMeta.ColumnMeta;
import com.fujitsu.hope.ds.TableMeta.ColumnMetaType;

public class PreparedStatementExecutorTest {
	private static Connection CONN;
	private static TableMeta ACCOUNT = 
			TableMeta.table("account")
			.key("id").type(ColumnMeta.INTEGER)
			.key("email").type(ColumnMeta.STRING)
			.column("profile1").type(ColumnMeta.STRING)
			.column("profile2").type(ColumnMeta.STRING)
			.column("profileNo").type(ColumnMeta.INTEGER)
			.meta();

	private static MapResover ACCOUNT_RESOLVER = new MapResover(ACCOUNT);

	static class MapResover implements PreparedStatementExecutor.ResultSetResolver<Map<String, Object>>{
		private final TableMeta meta;
		MapResover(TableMeta meta){
			this.meta = meta;
		}
		@Override
		public Map<String, Object> resolve(ResultSet rs) {
			Map<String, Object> map = new HashMap<String, Object>();
			for(String key : meta.keyNames()) map.put(key, valueOfType(key, meta.metaTypeOf(key), rs)); 
			for(String column : meta.columnNames()) map.put(column, valueOfType(column, meta.metaTypeOf(column), rs)); 
			return map;
		}
		
		private static Object valueOfType(String column, ColumnMetaType<?> type, ResultSet rs){
			try {
				if (type.equals(ColumnMeta.BOOLEAN)) return rs.getBoolean(column);
				else if (type.equals(ColumnMeta.BYTE)) return rs.getByte(column);
				else if (type.equals(ColumnMeta.DATE)) return rs.getDate(column);
				else if (type.equals(ColumnMeta.DOUBLE)) return rs.getDouble(column);
				else if (type.equals(ColumnMeta.FLOAT)) return rs.getFloat(column);
				else if (type.equals(ColumnMeta.INTEGER)) return rs.getInt(column);
				else if (type.equals(ColumnMeta.LONG)) return rs.getLong(column);
				else if (type.equals(ColumnMeta.SHORT)) return rs.getShort(column);
				else if (type.equals(ColumnMeta.STRING)) return rs.getString(column);
				else if (type.equals(ColumnMeta.BYTES)) return rs.getBytes(column);
				else throw new IllegalArgumentException("columnName "+column+" unsupported type " + type.getName());
			} catch (SQLException e) {
				throw new RuntimeException("ResultSetの値取得に失敗しています。", e);
			}
		}
	}
	
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
		SqliteUtils.get().ddl(CONN).create(ACCOUNT);
	} 
	
	@After
	public void tearDown() throws SQLException {
		SqliteUtils.get().ddl(CONN).drop(ACCOUNT);
	}
	
	@Test
	public void testTableMetaBuildCase001() throws InterruptedException, ExecutionException {
		Iterator<Map<String, Object>> ite = null;
		Map<String, Object> map = null;
		
		PreparedStatementExecutor executor = new PreparedStatementExecutor("test_executor", CONN);
		assertThat(executor.table(ACCOUNT).insert().set(1).set("hoge001@fuga.com").set("foo001").set("bar001").set(101).executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).insert().set(2).set("hoge002@fuga.com").set("foo002").set("bar002").set(102).executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).insert().set(3).set("hoge003@fuga.com").set("foo003").set("bar003").set(103).executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).insert().set(4).set("hoge004@fuga.com").set("foo004").set("bar004").set(104).executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).insert().set(5).set("hoge005@fuga.com").set("foo005").set("bar005").set(105).executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).insert().set(6).set("hoge006@fuga.com").set("foo006").set("bar006").set(106).executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).insert().set(7).set("hoge007@fuga.com").set("foo007").set("bar007").set(107).executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).insert().set(8).set("hoge008@fuga.com").set("foo008").set("bar008").set(108).executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).insert().set(9).set("hoge009@fuga.com").set("foo009").set("bar009").set(109).executeUpdate().get(), is(1));
		
		executor.transaction().commit();
		
		ite = executor.table(ACCOUNT).getEntity().
				set(1).set("hoge001@fuga.com").
				executeQuery().get().asIterator(ACCOUNT_RESOLVER);
		assertThat(ite.hasNext(), is(true));
		map = ite.next();
		assertThat(map.get("profile1"), is("foo001"));
		assertThat(map.get("profile2"), is("bar001"));
		assertThat(map.get("profileNo"), is(101));
		assertThat(ite.hasNext(), is(false));

		ite = executor.table(ACCOUNT).getEntity().
				set(9).set("hoge009@fuga.com").
				executeQuery().get().asIterator(ACCOUNT_RESOLVER);
	
		assertThat(ite.hasNext(), is(true));
		map = ite.next();
		assertThat(map.get("profile1"), is("foo009"));
		assertThat(map.get("profile2"), is("bar009"));
		assertThat(map.get("profileNo"), is(109));
		assertThat(ite.hasNext(), is(false));
		
		assertThat(executor.table(ACCOUNT).update().set("bar001").set("toe001").set(201).set(1).set("hoge001@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).update().set("bar002").set("toe002").set(202).set(2).set("hoge002@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).update().set("bar003").set("toe003").set(203).set(3).set("hoge003@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).update().set("bar004").set("toe004").set(204).set(4).set("hoge004@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).update().set("bar005").set("toe005").set(205).set(5).set("hoge005@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).update().set("bar006").set("toe006").set(206).set(6).set("hoge006@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).update().set("bar007").set("toe007").set(207).set(7).set("hoge007@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).update().set("bar008").set("toe008").set(208).set(8).set("hoge008@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).update().set("bar009").set("toe009").set(209).set(9).set("hoge009@fuga.com").executeUpdate().get(), is(1));

		ite = executor.table(ACCOUNT).getEntity().
				set(1).set("hoge001@fuga.com").
				executeQuery().get().asIterator(ACCOUNT_RESOLVER);
		assertThat(ite.hasNext(), is(true));
		map = ite.next();
		assertThat(map.get("profile1"), is("bar001"));
		assertThat(map.get("profile2"), is("toe001"));
		assertThat(map.get("profileNo"), is(201));
		assertThat(ite.hasNext(), is(false));

		ite = executor.table(ACCOUNT).getEntity().
				set(9).set("hoge009@fuga.com").
				executeQuery().get().asIterator(ACCOUNT_RESOLVER);
	
		assertThat(ite.hasNext(), is(true));
		map = ite.next();
		assertThat(map.get("profile1"), is("bar009"));
		assertThat(map.get("profile2"), is("toe009"));
		assertThat(map.get("profileNo"), is(209));
		assertThat(ite.hasNext(), is(false));

		assertThat(executor.table(ACCOUNT).delete().set(1).set("hoge001@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).delete().set(2).set("hoge002@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).delete().set(3).set("hoge003@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).delete().set(4).set("hoge004@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).delete().set(5).set("hoge005@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).delete().set(6).set("hoge006@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).delete().set(7).set("hoge007@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).delete().set(8).set("hoge008@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).delete().set(9).set("hoge009@fuga.com").executeUpdate().get(), is(1));

		ite = executor.table(ACCOUNT).getEntity().
				set(1).set("hoge001@fuga.com").
				executeQuery().get().asIterator(ACCOUNT_RESOLVER);
		assertThat(ite.hasNext(), is(false));

		ite = executor.table(ACCOUNT).getEntity().
				set(9).set("hoge009@fuga.com").
				executeQuery().get().asIterator(ACCOUNT_RESOLVER);
	
		assertThat(ite.hasNext(), is(false));
		
		executor.transaction().rollback();
		
		ite = executor.table(ACCOUNT).getEntity().
				set(1).set("hoge001@fuga.com").
				executeQuery().get().asIterator(ACCOUNT_RESOLVER);
		assertThat(ite.hasNext(), is(true));
		map = ite.next();
		assertThat(map.get("profile1"), is("foo001"));
		assertThat(map.get("profile2"), is("bar001"));
		assertThat(map.get("profileNo"), is(101));
		assertThat(ite.hasNext(), is(false));

		ite = executor.table(ACCOUNT).getEntity().
				set(9).set("hoge009@fuga.com").
				executeQuery().get().asIterator(ACCOUNT_RESOLVER);
	
		assertThat(ite.hasNext(), is(true));
		map = ite.next();
		assertThat(map.get("profile1"), is("foo009"));
		assertThat(map.get("profile2"), is("bar009"));
		assertThat(map.get("profileNo"), is(109));
		assertThat(ite.hasNext(), is(false));
	}
}