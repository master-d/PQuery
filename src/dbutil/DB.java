package dbutil;

import java.io.IOException;
import java.io.InputStream;
//import java.lang.annotation.Annotation;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.NamingException;
//import dbutil.annotations.DbColumn;

import dbutil.DBL;

/**
 * @author Rob Richards Created on 3/6/2013
 * updated 9/14/2016 to include custom resultset and preparestatement method to allow usage of named parameters.
 * The DB class is now standalone and does not expose the Connection, PreparedStatement, or ResultSet objects and 
 * will autoclose the connection after storing all data in the modified resultset class.  
 * The new ResultSet class holds all the rows returned from the query in a Queue<Map<String,Object>>
 */
public abstract class DB {
	private transient Connection con;
	private boolean isTransaction = false;
	private transient PreparedStatement ps;
	private Map<String,ArrayList<Integer>> namedParmMap = new HashMap<String,ArrayList<Integer>>();
	private boolean generatedKeysAvailable = false;
	
	
	protected DB(){	}
	/**
	 * initializes the database connection with provided schema id number. Static schema variables can be accessed using DB. or directly if your class extends the DB class
	 * @param idx (schema index number)
	 * @throws NamingException
	 * @throws SQLException
	 */
	protected final void beginTransaction() {
		this.isTransaction = true;
	}

	protected final void init(int idx) throws NamingException, SQLException{
		init(DBL.values()[idx]);
	}
	protected final void init(DBL dbl) throws NamingException, SQLException{
		if(con == null){
			//if(DEBUG){
				System.out.println("ITMDU3: Connecting to: " + dbl.name() + ":" + dbl.getInstance().getJndi());
			//}
			con = dbl.getInstance().getConnection();
			if (isTransaction)
				con.setAutoCommit(false);
		}
	}

	protected final void prepareStatement(String sql) throws SQLException {
		prepareStatement(sql, null);
	}
	protected final void prepareStatement(String sql, String[] generatedKeys) throws SQLException {
		this.generatedKeysAvailable = generatedKeys != null;
		// check for named parameters before creating the java.sql.preparedStatement
		Matcher m = Pattern.compile("'.*?'|:(\\w+)").matcher(sql);
		StringBuffer newsql = new StringBuffer();
		int idx=1;
		int sqlidx=0;
		while (m.find()) {
			String nparm = m.group(1);
			if (nparm != null) {
				newsql.append(sql.substring(sqlidx, m.start(1)-1) + "?");
				sqlidx = m.end();
				ArrayList<Integer> idxlist = namedParmMap.get(nparm);
				if (idxlist == null) {
					idxlist = new ArrayList<Integer>();
					idxlist.add(idx);
					namedParmMap.put(nparm, idxlist);
				} else {
					idxlist.add(idx);
				}
				idx++;
			}
		}
		if (sqlidx != 0) {
			newsql.append(sql.substring(sqlidx));
			sql = newsql.toString();
		}
			
		if (this.generatedKeysAvailable)
			ps = con.prepareStatement(sql, generatedKeys);
		else
			ps = con.prepareStatement(sql);
	}
	protected final void setParameter(int idx, Object value) throws SQLException {
		ps.setObject(idx, value);
	}
	protected final void setParameter(String name, Object value) throws SQLException {
		ArrayList<Integer> idxlist = namedParmMap.get(name);
		if (idxlist == null)
			throw new SQLException("Sql query contains no parameter named '" + name + "'");
		else {
			for (int idx: idxlist) {
				ps.setObject(idx, value);
			}
		}
	}
	protected final void setParameters(List<Object> parms) throws SQLException, IOException{
		Object[] parmArray = parms.toArray();
		System.out.println("Passed parameters:");
		
		for(int x = 0; x < parmArray.length; x++){
			Object parm = parmArray[x];
			//if (DEBUG) {
				if (parm == null)
					System.out.print("(" + (x+1) + ",null) ");
				else
					System.out.print("(" + (x+1) + "," +parm.getClass().getSimpleName() + "," + parm + ") ");
			//}
			if (parm == null) {
				ps.setString(x+1, null);
				//ps.setNull(x+1, java.sql.Types.NULL);
			} else {
			Class<?> cls = parm.getClass();
			if(cls == String.class){
				ps.setString(x + 1, parm.toString());
			}
			else if(cls == Integer.class){
				ps.setInt(x + 1, (Integer) parm);
			}
			else if (Date.class.isAssignableFrom(cls)) {
				java.sql.Timestamp val = new java.sql.Timestamp(((Date) parm).getTime());
				ps.setTimestamp(x + 1, val);
			}
			else if(cls == Long.class) {
				ps.setLong(x + 1, (Long) parm);
			}
			else if(cls == Double.class){
				ps.setDouble(x + 1, (Double) parm);
			}
			else if(cls == Boolean.class){
				ps.setString(x + 1, (Boolean) parm ? "Y" : "N");
			}
			else if(cls == InputStream.class){
				Blob blob = con.createBlob();
				blob.setBytes(1,DbUtility.inputStreamToByteArray((InputStream) parm));
				ps.setBlob(x + 1, blob);
			}
			else if(cls == Byte[].class || cls == byte[].class){
				byte[] b = (byte[]) parm;
				Blob blob = con.createBlob();
				blob.setBytes(1,b);
				ps.setBlob(x + 1, blob);
				
			}
			else{
				ps.setString(x + 1, parm.toString());
			}
			}
		}
	}
	protected final dbutil.ResultSet executeQuery() throws SQLException {
		dbutil.ResultSet rsc = new dbutil.ResultSet();
		ResultSet rs = null;
		try {
			rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			while (rs.next()) {
				Map<String,Object> rsmap = new HashMap<String, Object>();
				for (int x=1; x<=rsmd.getColumnCount(); x++) {
					String colname = rsmd.getColumnName(x).toUpperCase();
					//ItmdLog.log("col name:" + colname);
					rsmap.put(colname, rs.getObject(x));
				}
				rsc.push(rsmap);
			}
		}
		finally {
			closeQuietly(rs);
			closeQuietly();
		}
		return rsc;
	}
	
	protected final int executeUpdate() throws SQLException {
		try {
			return ps.executeUpdate();
		}
		finally {
			closeQuietly();
		}
	}
	protected final List<Object> executeInsert() throws SQLException {
		List<Object> keys = null;
		try {
			int ct = ps.executeUpdate();
			if (ct < 1)
				throw new SQLException("Unable to insert row");
			else
				keys = getGeneratedKeyValues();
		} finally {
			closeQuietly();
		}
		return keys;
	}
	public List<Object> getGeneratedKeyValues() throws SQLException {
		List<Object> keys = new LinkedList<Object>();
		if (this.generatedKeysAvailable) {
			try {
				int idx=0;
				ResultSet gkrs = ps.getGeneratedKeys();
				while (gkrs.next()) {
					keys.add(gkrs.getObject(++idx));
				}
			}
			finally {
				closeQuietly();
			}
		}
		return keys;
	}
	protected final void setAutoCommit(boolean flag) throws SQLException{
		con.setAutoCommit(flag);
	}
	protected final void closeQuietly(){		
		if(ps != null){
			try{
				ps.close();
			}
			catch(Exception e){
			}
		}
		try{
			if(!isTransaction){
				con.close();
				con = null;
			}
		}
		catch(Exception e){
		}
	}
	protected final void closeQuietly(ResultSet rs){
		if(rs != null){
			try{
				rs.close();
			}
			catch(Exception e){
			}
		}
	}
	protected void closeQuietly(Statement ps){
		if(ps != null){
			try{
				ps.close();
			}
			catch(Exception e){
			}
		}
	}
	protected final void endTransaction(){
		endTransaction(con);
	}
	protected final void endTransaction(Connection conn){
		try{
			conn.setAutoCommit(true);
			conn.close();
			conn = null;
		}
		catch(Exception e){
		}
	}
	
	protected final void rollback() throws SQLException {
		con.rollback();
	}
	protected final void commit() throws SQLException {
		con.commit();
	}
	

}