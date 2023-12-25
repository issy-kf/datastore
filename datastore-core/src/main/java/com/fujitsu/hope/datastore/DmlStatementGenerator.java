package com.fujitsu.hope.datastore;

import com.fujitsu.hope.datastore.TableMeta.ColumnMeta;

class DmlStatementGenerator {
	private static String[] columnToStringArray(ColumnMeta[] meta) {
		String[] strings = new String[meta.length];
		for(int i=0; i<strings.length; i++)
			strings[i] = meta[i].getName();
		return strings;
	}
	
	
	/**
	 * generate statement:
	 *  "select key1, key2, property1, property2 from table_name"
	 * @param meta
	 * @return String: statement
	 */
	static String getEntity(TableMeta meta){
		String[] selectArray = StatementGenerateHelper.joinArrays(columnToStringArray(meta.keys()),columnToStringArray(meta.attributes()));
		String select = "select " + StatementGenerateHelper.connectString(", ", selectArray) + " ";
		String from = "from " + meta.tableName() + " ";
		String where = StatementGenerateHelper.where(columnToStringArray(meta.keys()));
		return select + from + where;
	}
	
	/**
	 * generate statement:
	 *  "select key1, key2 from table_name"
	 * @param meta
	 * @return String: statement
	 */
	static String selectKeys(TableMeta meta, String statementWhereOrderby){
		return "select " + 
				StatementGenerateHelper.connectString(", ", columnToStringArray(meta.keys())) + " " + 
				"from " + meta.tableName() + " " + statementWhereOrderby;
	}
	
	/**
	 * generate statement:
	 * 	"delete from table_name 
	 *  	where key[0]=? and key[1]=? and ... "
	 * @param meta
	 * @return String: statement
	 */
	static String delete(TableMeta meta){
		return "delete from " + meta.tableName() + " " 
				+ StatementGenerateHelper.where(columnToStringArray(meta.keys())); 
	}
	
	/**
	 * generate statement:
	 * 	"update table_name 
	 * 		set (property[0]=?, property[1]=?, ... , property[n-1]=?)
	 *  	where key[0]=? and key[1]=? and ... "
	 * @param meta
	 * @return String: statement
	 */
	static String update(TableMeta meta){
		return "update " + meta.tableName() + " " 
				+ StatementGenerateHelper.set(columnToStringArray(meta.attributes())) + " " 
				+ StatementGenerateHelper.where(columnToStringArray(meta.keys()));
	}
	
	/**
	 * generate statement:
	 * 	"insert into 
	 * 		table_name (keyName[0], keyName[1], propertyName[0], ... ) 
	 * 		values (?, ?, ?)"
	 * @param meta
	 * @return String: statement
	 */
	static String insert(TableMeta meta){
		return "insert " + 
				StatementGenerateHelper.into(meta) + " " + 
				StatementGenerateHelper.values(meta);
	}
	
	static class StatementGenerateHelper {
		/**
		 * generate statement:
		 * 	"set (args[0]=? and args[1]=? and args[0]=?)"
		 * @param args
		 * @return
		 */
		static String set(String... args){
			String[] addEquals = arrayAdditionalEquals(args);
			return "set " + connectString(", ", addEquals);
		}
		
		/**
		 * generate statement:
		 * 	"where args[0]=? and args[1]=? and args[0]=?"
		 * @param args
		 * @return
		 */
		static String where(String... args){
			String[] addEquals = arrayAdditionalEquals(args);
			return "where " + connectString(" and ", addEquals);
		}
		
		
		static String into(TableMeta meta){
			String[] joined = joinArrays(columnToStringArray(meta.keys()), columnToStringArray(meta.attributes()));
			return "into "
					+ meta.tableName() + " "
					+ inParentheses(connectString(", ", joined));
		}
		
		/**
		 * generate bind parameter String of "Insert".
		 * [?, ?, ?, ?, ?]
		 * @param meta
		 * @return
		 */
		static String values(TableMeta meta){
			int valuesLength = meta.attributes().length + meta.keys().length;
			String[] bindStrings = new String[valuesLength];
			for(int i=0; i<valuesLength; i++)  bindStrings[i] = "?";
			return "values " + inParentheses(connectString(", ", bindStrings));
		}
		
		/**
		 * 
		 * @param values
		 * @return
		 * 		[values[0]=?,values[1]=?,,,values[n-1]=?]
		 */
		static String[] arrayAdditionalEquals(String... values){
			String[] result = new String[values.length];
			for(int i=0; i<values.length; i++) result[i] = values[i] + "=?";
			return result;
		}
		
		/**
		 * connect String by connector 
		 * @param 
		 * @return
		 */
		static String connectString(String connector, String... values){
			StringBuilder sb = new StringBuilder();
			boolean first = true;
			for(String value : values){
				if(first) first = false; 
				else  sb.append(connector);
				sb.append(value);
			}
			return sb.toString();
		}
		
		
		static String inParentheses(String source){
			StringBuilder sb = new StringBuilder();
			sb.append("(");
			sb.append(source);
			sb.append(")");
			return sb.toString();
		}
		
		/**
		 * @param a
		 * @param b
		 * @return
		 */
		static String[] joinArrays(String[] a, String[] b){
			int length = a.length + b.length;
			String[] arrays = new String[length];
			for(int i=0; i<a.length; i++) arrays[i] = a[i];
			for(int i=0; i<b.length; i++) arrays[i+a.length] = b[i];
			return arrays;
		}
	}
}
