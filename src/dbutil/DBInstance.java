package dbutil;

import java.sql.Connection;
import java.sql.SQLException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.sql.DataSource;

public final class DBInstance{
	public static final int ITMDAPPS = 0, MINTAPPS = 1, MINTANDITMD = 2, ALL = 3, NONE = 4;
	private String test;
	private String prod;
	private int prodLocation = DBInstance.ITMDAPPS;
	public DBInstance(){
	}
	/**
	 * @param test
	 * @param prod
	 * @param prodLocation
	 */
	public DBInstance(String test, String prod, int prodLocation){
		this.test = test;
		this.prod = prod;
		this.prodLocation = prodLocation;
	}
	/**
	 * @param test
	 * @param prod
	 */
	public DBInstance(String test, String prod){
		this.test = test;
		this.prod = prod;
	}
	// use this constructor if there is no test database available
	// it is the equivalent of doing new DBLookup(prod, test, DBLookup.ALL)
	public DBInstance(String prod){
		prodLocation = DBInstance.ALL;
		this.prod = prod;
		test = prod;
	}
	/**
	 * @return alias to the test database
	 */
	public String getTest(){
		return test;
	}
	public void setTest(String test){
		this.test = test;
	}
	/**
	 * @return alias to the production database
	 */
	public String getProd(){
		return prod;
	}
	public void setProd(String prod){
		this.prod = prod;
	}
	/**
	 * @return the prod location ID number which is used to determine when the prod db connection is used
	 */
	public int getProdLocation(){
		return prodLocation;
	}
	public void setProdLocation(int prodLocation){
		this.prodLocation = prodLocation;
	}
	public String getJndi(){
		return prod;
	}
	public Connection getConnection() throws NamingException, SQLException{
		Context ctx = new InitialContext();
		DataSource ds = null;
		ds = (DataSource) ctx.lookup(getJndi());
		if(ds == null){
			throw new NameNotFoundException("Could not get DS connection for - " + getJndi());
		}
		Connection con = ds.getConnection();
		ctx.close();
		return con;
	}
}