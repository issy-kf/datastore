package com.fujitsu.hope.datastore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fujitsu.hope.datastore.TableMeta;
import com.fujitsu.hope.datastore.TableMeta.ColumnMeta;
import com.fujitsu.hope.datastore.TableMeta.ColumnMetaType;

class SqliteUtils {
	private static final String JDBC_CLASS = "org.sqlite.JDBC"; 
	private static SqliteUtils singleton = new SqliteUtils();
	SqliteUtils(){
		try {
			Class.forName(JDBC_CLASS);
		} catch (ClassNotFoundException e) {
			throw new Error("Sqliteが見つかりません", e);
		}
	}
	
	static SqliteUtils get() {
		return singleton;
	}
	
	Connection createConnectionOnMemory() throws SQLException {
		Connection connection = DriverManager.getConnection("jdbc:sqlite::memory:");
		connection.setAutoCommit(false);
		return connection;
	}
	
	DdlExecutor ddl(Connection conn) { return new DdlExecutor(conn); }
	
	class DdlExecutor {
		private Connection conn;
		DdlExecutor (Connection conn){ this.conn = conn; }
		void create(TableMeta table) throws SQLException {
			exec(SqliteStatementHelper.create(table));
		}
		void drop(TableMeta table) throws SQLException {
			exec(SqliteStatementHelper.drop(table));
		}
		private void exec(String sql) throws SQLException {
			conn.createStatement().execute(sql);
		}
	}
	
	private class SqliteStatementHelper {
		private static enum SQLITE_COLUMN_TYPE{
			INTEGER,
			TEXT,
			REAL,
			BLOB,
			NULL
		}
		
		private static final Map<ColumnMetaType<?>, SQLITE_COLUMN_TYPE> TYPE_MAP = 
				new HashMap<ColumnMetaType<?>, SQLITE_COLUMN_TYPE>();
		
		static {
			TYPE_MAP.put(ColumnMeta.INTEGER, SQLITE_COLUMN_TYPE.INTEGER);
			TYPE_MAP.put(ColumnMeta.LONG,    SQLITE_COLUMN_TYPE.INTEGER);
			TYPE_MAP.put(ColumnMeta.SHORT,   SQLITE_COLUMN_TYPE.INTEGER);
			TYPE_MAP.put(ColumnMeta.BYTE, 	SQLITE_COLUMN_TYPE.INTEGER);
			TYPE_MAP.put(ColumnMeta.DOUBLE,  SQLITE_COLUMN_TYPE.REAL);
			TYPE_MAP.put(ColumnMeta.FLOAT,   SQLITE_COLUMN_TYPE.REAL);
			TYPE_MAP.put(ColumnMeta.STRING,  SQLITE_COLUMN_TYPE.TEXT);
			TYPE_MAP.put(ColumnMeta.DATE,    SQLITE_COLUMN_TYPE.INTEGER);
			TYPE_MAP.put(ColumnMeta.BOOLEAN, SQLITE_COLUMN_TYPE.NULL);
		}
				
		static String create(TableMeta p){
			StringBuilder sb = new StringBuilder();
			sb.append("create table ");
			sb.append(p.tableName());
			sb.append(" (");
			boolean first = true;
			
			// 
			Set<String> keySet = new HashSet<String>();
			for(ColumnMeta<?> meta : p.keys()) keySet.add(meta.getName());
			
			// add all key and all column; 
			List<ColumnMeta<?>> columnList = new ArrayList<ColumnMeta<?>>();
			for(ColumnMeta<?> meta : p.keys()) 
				columnList.add(meta);
			for(ColumnMeta<?> meta : p.properties()) 
				columnList.add(meta);
			
			for(ColumnMeta<?> column : columnList){
				if(first) first = false;
				else sb.append(", ");
				
				sb.append(column.getName());
				
				if(column.getType() != null)
					if(TYPE_MAP.containsKey(column.getType()))
						sb.append(" "+TYPE_MAP.get(column.getType()).toString());
				
				if(keySet.contains(column.getName()))
					sb.append(" primarykey");
			}
			sb.append(")");
			
			return sb.toString();
		}
		
		static String drop (TableMeta meta) {
			StringBuilder sb = new StringBuilder();
			sb.append("drop table ");
			sb.append(meta.tableName());
			return sb.toString();		
		}
	}
}
