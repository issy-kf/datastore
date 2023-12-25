package com.fujitsu.hope.datastore;

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

import com.fujitsu.hope.datastore.TableMeta.ColumnMeta;
import com.fujitsu.hope.datastore.meta.ColumnType;

public class PreparedStatementExecutorTest {
	private static Connection CONN;
	private static TableMeta ACCOUNT = 
			TableMeta.table("account")
			.key("id").type(ColumnType.INTEGER)
			.key("email").type(ColumnType.STRING)
			.attribute("profile1").type(ColumnType.STRING)
			.attribute("profile2").type(ColumnType.STRING)
			.attribute("profileNo").type(ColumnType.STRING)
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
			for(ColumnMeta key : meta.keys()) map.put(key.getName(), valueOfType(key.getName(), key.getType(), rs)); 
			for(ColumnMeta column : meta.attributes()) map.put(column.getName(), valueOfType(column.getName(), column.getType(), rs)); 
			return map;
		}
		
		private static Object valueOfType(String column, ColumnType type, ResultSet rs){
			try {
				if (type.equals(ColumnType.BOOLEAN)) return rs.getBoolean(column);
				else if (type.equals(ColumnType.BYTE)) return rs.getByte(column);
				else if (type.equals(ColumnType.DATE)) return rs.getDate(column);
				else if (type.equals(ColumnType.DOUBLE)) return rs.getDouble(column);
				else if (type.equals(ColumnType.FLOAT)) return rs.getFloat(column);
				else if (type.equals(ColumnType.INTEGER)) return rs.getInt(column);
				else if (type.equals(ColumnType.LONG)) return rs.getLong(column);
				else if (type.equals(ColumnType.SHORT)) return rs.getShort(column);
				else if (type.equals(ColumnType.STRING)) return rs.getString(column);
				else if (type.equals(ColumnType.BYTES)) return rs.getBytes(column);
				else throw new IllegalArgumentException("columnName "+column+" unsupported type " + type);
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
		assertThat(executor.table(ACCOUNT).insert().setInteger(1).setString("hoge001@fuga.com").setString("foo001").setString("bar001").setInteger(101).executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).insert().setInteger(2).setString("hoge002@fuga.com").setString("foo002").setString("bar002").setInteger(102).executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).insert().setInteger(3).setString("hoge003@fuga.com").setString("foo003").setString("bar003").setInteger(103).executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).insert().setInteger(4).setString("hoge004@fuga.com").setString("foo004").setString("bar004").setInteger(104).executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).insert().setInteger(5).setString("hoge005@fuga.com").setString("foo005").setString("bar005").setInteger(105).executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).insert().setInteger(6).setString("hoge006@fuga.com").setString("foo006").setString("bar006").setInteger(106).executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).insert().setInteger(7).setString("hoge007@fuga.com").setString("foo007").setString("bar007").setInteger(107).executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).insert().setInteger(8).setString("hoge008@fuga.com").setString("foo008").setString("bar008").setInteger(108).executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).insert().setInteger(9).setString("hoge009@fuga.com").setString("foo009").setString("bar009").setInteger(109).executeUpdate().get(), is(1));
		
		executor.transaction().commit();
		
		ite = executor.table(ACCOUNT).getEntity().
				setInteger(1).setString("hoge001@fuga.com").
				executeQuery().get().asIterator(ACCOUNT_RESOLVER);
		assertThat(ite.hasNext(), is(true));
		map = ite.next();
		assertThat(map.get("profile1"), is("foo001"));
		assertThat(map.get("profile2"), is("bar001"));
		assertThat(map.get("profileNo"), is("101"));
		assertThat(ite.hasNext(), is(false));

		ite = executor.table(ACCOUNT).getEntity().
				setInteger(9).setString("hoge009@fuga.com").
				executeQuery().get().asIterator(ACCOUNT_RESOLVER);
	
		assertThat(ite.hasNext(), is(true));
		map = ite.next();
		assertThat(map.get("profile1"), is("foo009"));
		assertThat(map.get("profile2"), is("bar009"));
		assertThat(map.get("profileNo"), is("109"));
		assertThat(ite.hasNext(), is(false));
		
		assertThat(executor.table(ACCOUNT).update().setString("bar001").setString("toe001").setInteger(201).setInteger(1).setString("hoge001@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).update().setString("bar002").setString("toe002").setInteger(202).setInteger(2).setString("hoge002@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).update().setString("bar003").setString("toe003").setInteger(203).setInteger(3).setString("hoge003@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).update().setString("bar004").setString("toe004").setInteger(204).setInteger(4).setString("hoge004@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).update().setString("bar005").setString("toe005").setInteger(205).setInteger(5).setString("hoge005@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).update().setString("bar006").setString("toe006").setInteger(206).setInteger(6).setString("hoge006@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).update().setString("bar007").setString("toe007").setInteger(207).setInteger(7).setString("hoge007@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).update().setString("bar008").setString("toe008").setInteger(208).setInteger(8).setString("hoge008@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).update().setString("bar009").setString("toe009").setInteger(209).setInteger(9).setString("hoge009@fuga.com").executeUpdate().get(), is(1));

		ite = executor.table(ACCOUNT).getEntity().
				setInteger(1).setString("hoge001@fuga.com").
				executeQuery().get().asIterator(ACCOUNT_RESOLVER);
		assertThat(ite.hasNext(), is(true));
		map = ite.next();
		assertThat(map.get("profile1"), is("bar001"));
		assertThat(map.get("profile2"), is("toe001"));
		assertThat(map.get("profileNo"), is("201"));
		assertThat(ite.hasNext(), is(false));

		ite = executor.table(ACCOUNT).getEntity().
				setInteger(9).setString("hoge009@fuga.com").
				executeQuery().get().asIterator(ACCOUNT_RESOLVER);
	
		assertThat(ite.hasNext(), is(true));
		map = ite.next();
		assertThat(map.get("profile1"), is("bar009"));
		assertThat(map.get("profile2"), is("toe009"));
		assertThat(map.get("profileNo"), is("209"));
		assertThat(ite.hasNext(), is(false));

		assertThat(executor.table(ACCOUNT).delete().setInteger(1).setString("hoge001@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).delete().setInteger(2).setString("hoge002@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).delete().setInteger(3).setString("hoge003@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).delete().setInteger(4).setString("hoge004@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).delete().setInteger(5).setString("hoge005@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).delete().setInteger(6).setString("hoge006@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).delete().setInteger(7).setString("hoge007@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).delete().setInteger(8).setString("hoge008@fuga.com").executeUpdate().get(), is(1));
		assertThat(executor.table(ACCOUNT).delete().setInteger(9).setString("hoge009@fuga.com").executeUpdate().get(), is(1));

		ite = executor.table(ACCOUNT).getEntity().
				setInteger(1).setString("hoge001@fuga.com").
				executeQuery().get().asIterator(ACCOUNT_RESOLVER);
		assertThat(ite.hasNext(), is(false));

		ite = executor.table(ACCOUNT).getEntity().
				setInteger(9).setString("hoge009@fuga.com").
				executeQuery().get().asIterator(ACCOUNT_RESOLVER);
	
		assertThat(ite.hasNext(), is(false));
		
		executor.transaction().rollback();
		
		ite = executor.table(ACCOUNT).getEntity().
				setInteger(1).setString("hoge001@fuga.com").
				executeQuery().get().asIterator(ACCOUNT_RESOLVER);
		assertThat(ite.hasNext(), is(true));
		map = ite.next();
		assertThat(map.get("profile1"), is("foo001"));
		assertThat(map.get("profile2"), is("bar001"));
		assertThat(map.get("profileNo"), is("101"));
		assertThat(ite.hasNext(), is(false));

		ite = executor.table(ACCOUNT).getEntity().
				setInteger(9).setString("hoge009@fuga.com").
				executeQuery().get().asIterator(ACCOUNT_RESOLVER);
	
		assertThat(ite.hasNext(), is(true));
		map = ite.next();
		assertThat(map.get("profile1"), is("foo009"));
		assertThat(map.get("profile2"), is("bar009"));
		assertThat(map.get("profileNo"), is("109"));
		assertThat(ite.hasNext(), is(false));
	}
}