package com.fujitsu.hope.ds;

class DmlStatementGenerator {
	protected StatementGenerateHelper helper = new StatementGenerateHelper();
	
	public String entity(TableMeta meta){
		return "select " + 
				helper.connectString(", ", helper.joinArrays(meta.keyNames(), meta.columnNames())) + " " + 
				"from " + meta.tableName() + " ";
	}
	
	public String key(TableMeta meta){
		return "select " + 
				helper.connectString(", ", meta.keyNames()) + " " + 
				"from " + meta.tableName() + " ";
	}
	
	/**
	 * generate statement:
	 * 	"delete from table_name 
	 *  	where key[0]=? and key[1]=? and ... "
	 * @param meta
	 * @return
	 */
	public String delete(TableMeta meta){
		return "delete from " + meta.tableName() + " " + helper.where(meta.keyNames()); 
	}
	
	/**
	 * generate statement:
	 * 	"update table_name 
	 * 		set (property[0]=?, property[1]=?, ... , property[n-1]=?)
	 *  	where key[0]=? and key[1]=? and ... "
	 * @param meta
	 * @return
	 */
	public String update(TableMeta meta){
		return "update " + meta.tableName() + " " 
				+ helper.set(meta.columnNames()) + " " 
				+ helper.where(meta.keyNames());
	}
	
	/**
	 * generate statement:
	 * 	"insert into 
	 * 		table_name (keyName[0], keyName[1], propertyName[0], ... ) 
	 * 		values (?, ?, ?)"
	 * @param meta
	 * @return
	 */
	public String insert(TableMeta meta){
		return "insert " + 
				helper.into(meta) + " " + 
				helper.values(meta);
	}
	
	static class StatementGenerateHelper {
		/**
		 * generate statement:
		 * 	"set (args[0]=? and args[1]=? and args[0]=?)"
		 * @param args
		 * @return
		 */
		String set(String... args){
			String[] addEquals = arrayAdditionalEquals(args);
			return "set " + connectString(", ", addEquals);
		}
		
		/**
		 * generate statement:
		 * 	"where args[0]=? and args[1]=? and args[0]=?"
		 * @param args
		 * @return
		 */
		String where(String... args){
			String[] addEquals = arrayAdditionalEquals(args);
			return "where " + connectString(" and ", addEquals);
		}
		
		
		String into(TableMeta meta){
			String[] joined = joinArrays(meta.keyNames(), meta.columnNames());
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
		String values(TableMeta meta){
			int valuesLength = meta.columnNames().length + meta.keyNames().length;
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
		String[] arrayAdditionalEquals(String... values){
			String[] result = new String[values.length];
			for(int i=0; i<values.length; i++) result[i] = values[i] + "=?";
			return result;
		}
		
		/**
		 * connect String by connector 
		 * @param 
		 * @return
		 */
		String connectString(String connector, String... values){
			StringBuilder sb = new StringBuilder();
			boolean first = true;
			for(String value : values){
				if(first) first = false; 
				else  sb.append(connector);
				sb.append(value);
			}
			return sb.toString();
		}
		
		
		String inParentheses(String source){
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
		String[] joinArrays(String[] a, String[] b){
			int length = a.length + b.length;
			String[] arrays = new String[length];
			for(int i=0; i<a.length; i++) arrays[i] = a[i];
			for(int i=0; i<b.length; i++) arrays[i+a.length] = b[i];
			return arrays;
		}
	}
}
