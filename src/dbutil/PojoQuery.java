package dbutil;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.naming.NamingException;
import java.security.InvalidParameterException;


import dbutil.DBField;
import dbutil.ObjectConverter;
import dbutil.annotations.DbColumn;
import dbutil.annotations.DbTable;
import dbutil.annotations.Id;
import dbutil.annotations.JoinTable;
import dbutil.annotations.PojoSecurity;

/**
 * @author Rob Richards Created on 6/16/2016
 */
 @SuppressWarnings({"unchecked"})
public class PojoQuery<T> extends DB{
	private Class<T> cls;
	private T obj = null;
	private String origSql = null;
	// List containing passed parameters for the query
	private List<Object> parms = new ArrayList<Object>();
	// List contains join fields specified in the 'select' statement
	private List<String> joinFields = new ArrayList<String>();
	// Set contains join statements that will be added to the query when a join is specified in the where clause
	private Set<String> joinstmts = new LinkedHashSet<String>();
	// Map contains individual where conditions (key) and what to replace them with (value)
	private Map<String, String> whereReplacements = new HashMap<String, String>();
	// User that security will be checked against if enabled
	private boolean securityEnabled = true;
	private PqUser securityUser = null;
	// indicates whether the query will retrieve blob or clob data when run
	private boolean retrieveBlobs = false;

	//private Object[] parms;
	private Set<Field> selectedFields = new LinkedHashSet<Field>();
	private static enum QueryType { select, count, insert, update, delete };
	// This array is used to store regular expressions for short hand code that can be used in a custom where clause
	// passed in by an application to illustrate a larger concept.
	// (example: where clause = "id=4 limit(1, 10)" will generate results for the first page with 10 results per page
	private static final transient String[] keywords = {"(?s)^(.*)limit\\s*\\(\\s*(\\d+),\\s*(\\d+)\\s*\\)(.*)$"};
//	private static final transient String[] query_templates = {"select %s from %s ", "select count(*) from %s", "insert into %s", "update %s", "delete from %s"};
//	private static final transient int SELECT = 0, COUNT = 1, INSERT = 2, UPDATE = 3, DELETE = 4;
	public PojoQuery(){ }
	public PojoQuery(Class<T> cls) {
		this.cls = cls;
	}

	public PojoQuery(Class<T> cls, String sql, Object... parms){
		this.cls = cls;
		// this.cls = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
		addParms(parms);
		this.origSql = sql;
	}

	public PojoQuery(T obj) throws IllegalArgumentException{
		this.cls = (Class<T>) obj.getClass();
		this.obj = obj;
	}
	public PojoQuery(T obj, String sql, Object... parms) {
		this.cls = (Class<T>) obj.getClass();
		this.obj = obj;
		addParms(parms);
		this.origSql = sql;
	}
	
	private String createSql(QueryType qt) throws NoSuchFieldException, NumberFormatException, IllegalAccessException, InstantiationException, SQLException, NamingException, IOException {
		String sql = origSql == null ? "" : origSql;
		// look for keywords in the query
		boolean hasLimit = false;
		for (int x=0; x<keywords.length; x++) {
			if (sql.matches(keywords[x])) {
				if (x==0) {
					hasLimit = true;
					String[] pagedata = sql.replaceAll(keywords[x], "$2,$3").split(",");
					sql = sql.replaceAll(keywords[x], "$1 $4");
					Integer page_num = Integer.parseInt(pagedata[0]);
					Integer num_per_page = Integer.parseInt(pagedata[1]);
					parms.add((page_num * num_per_page) - (num_per_page - 1));
					parms.add((page_num * num_per_page));
				}
			}
		}
		String select = "";
		String where = "";
		String orderby = "";
		String [] query = sql.split("\\s?where\\s");
		select = query[0].replaceAll("^(.*?)\\s+from\\s+.*$", "$1");
		//select = query.length > 0 ? query[0].replaceAll("^(.*?)\\s+from\\s+.*$", "$1") : sql;
		if(query.length > 1){
			where = "where " + query[1];
		}
		query = select.split("\\s?order\\sby\\s");
		select = query[0];
		//select = query.length > 0 ? query[0] : select;
		if (query.length > 1) {
			orderby = "order by " + genOrderBy(query[1]);
		}
		String from = genFromStmt();
		select = qt.ordinal() == QueryType.count.ordinal() ? genCountStmt() : genSelectStmt(select);
		// indicates a 'retrieve with joins' if a jointable field is found in the where clause
		where = genJoinAndWhereStmts(cls, where);
		sql = select + " \n" + from + " \n" + where + "\n" + orderby;
		if (hasLimit)
			sql = "SELECT outer.* FROM (SELECT ROWNUM rn, inner.* FROM (" + sql + ") inner) outer WHERE outer.rn >= ? AND outer.rn <= ?";
		return sql;
	}
	private String createSqlFromObj(QueryType qt) throws IllegalArgumentException, IllegalAccessException, InstantiationException, SQLException, NamingException, IOException, NoSuchFieldException{
		StringBuffer sql = new StringBuffer();
		Set<Field> fields = getFieldsWithAnnotation(DbColumn.class, this.cls);
		// generate select query 
		if (qt == QueryType.select) {
			if (this.origSql != null && this.origSql.matches("^.*where\\s+.*$"))
				throw new IllegalArgumentException("You cannot pass a where clause and an object when running a select query. Please use the PojoQuery<?>(Class<?> cls, String sql) constructor instead");
			sql.append(genSelectStmt(this.origSql) + " \n");
			sql.append(genFromStmt() + " \nwhere ");
			int fieldsSet = 0;
			for(Field f : fields) {
				f.setAccessible(true);
				Object val = f.get(obj);
				if (isFieldInitialized(f, val)) {
					fieldsSet++;
					DbColumn col = f.getAnnotation(DbColumn.class);
					if(parms.size() > 0){
						sql.append(" and ");
					}
					sql.append(col.value() + "=?");
					parms.add(val);
				}
			}
			if (fieldsSet == 0)
				throw new IllegalArgumentException("Cannot run select query against an object that doesn't have any fields set to a non-null value");
		} else if (qt == QueryType.insert) {
			if (insertAllowed()) {
				StringBuffer values = new StringBuffer();
				DbTable tbl = cls.getAnnotation(DbTable.class);
				sql.append("insert into " + tbl.value() + "(");
				for(Field f : fields) {
					f.setAccessible(true);
					Object val = f.get(obj);
					DbColumn col = f.getAnnotation(DbColumn.class);
					// check for a sequence
					Id id = f.getAnnotation(Id.class);
					boolean hasSequence = id != null && !"".equals(id.sequence());
					boolean fieldInitialized = isFieldInitialized(f, val);
					if (insertFieldAllowed(f) || hasSequence) {
						if (fieldInitialized) {
							sql.append(col.value() + ",");
							values.append("?,");
							parms.add(val);
						} else if (hasSequence) {
							sql.append(col.value() + ",");
							values.append(id.sequence() + ".nextval" + ",");
						}
					}
				}
				if (values.length() == 0)
					throw new IllegalArgumentException("Nothing to insert");
				sql.setLength(sql.length()-1);
				values.setLength(values.length()-1);
				sql.append(") \nvalues(" + values.toString() + ")");
			}
		} else if (qt == QueryType.delete) {
			if (deleteAllowed()) {
				DbTable tbl = cls.getAnnotation(DbTable.class);
				sql.append("delete from " + tbl.value());
				// delete by id if no where clause is passed
				if (this.origSql == null) {
					Set<Field> idfields = getFieldsWithAnnotation(Id.class, this.cls);
					if (idfields.size() == 0) {
						throw new SQLException("Cannot delete from " + cls.getSimpleName() + " because it does not have any @Id fields set");
					}
					else {
						sql.append(" where ");
					}
					for(Field f: idfields){
						f.setAccessible(true);
						if(parms.size() > 0){
							sql.append(" and ");
						}
						DbColumn col = f.getAnnotation(DbColumn.class);
						sql.append(col.value() + "= ? " );
						parms.add(f.get(obj));
					}
				} else if (this.origSql.matches("^\\s*where\\s+.*$")) {
					sql.append(genJoinAndWhereStmts(cls, this.origSql));
				} else {
					throw new IllegalArgumentException("You must supply a valid where clause before performing a delete operation");
				}
			}
		} else
			throw new IllegalAccessException("You are not allowed to delete rows from " + obj.getClass().getName());
		return sql.toString();
	}

	public PojoQuery<T> addParm(Object parm) {
		this.parms.add(parm);
		return this;
	}
	private PojoQuery<T> addParms(Object[] parms) {
		for (Object o: parms) {
			this.parms.add(o);
		}
		return this;
	}
	public PojoQuery<T> setSecurityUser(PqUser user) {
		this.securityEnabled = true;
		this.securityUser = user;
		return this;
	}
	
	public PojoQuery<T> disableSecurity() {
		this.securityEnabled = false;
		return this;
	}

	public T single() throws NamingException, SQLException, IOException, IllegalAccessException, InstantiationException, NoSuchFieldException, InvalidParameterException{
		this.retrieveBlobs = true;
		Collection<T> coll = executeSelect();
		if(coll.size() == 0){
			return null;
		}
		else{
			return coll.iterator().next();
		}
	}
	public long count() throws NumberFormatException, IllegalArgumentException, SQLException, NoSuchFieldException, IllegalAccessException, InstantiationException, NamingException, IOException, InvalidParameterException {
		long ct = executeCount();
		return ct;
	}
	public List<T> list() throws NamingException, SQLException, IOException, IllegalAccessException, InstantiationException, NoSuchFieldException, InvalidParameterException{
		List<T> results = new ArrayList<T>(executeSelect());
		return results;
	}
	public Set<T> set() throws NamingException, SQLException, IOException, IllegalAccessException, InstantiationException, NoSuchFieldException, InvalidParameterException{
		Set<T> results = new TreeSet<T>(executeSelect());
		return results;
	}
	public Map<?, T> map() throws NamingException, SQLException, IOException, IllegalAccessException, InstantiationException, NoSuchFieldException, InvalidParameterException{
		Collection<T> coll = executeSelect();
		Map<Object, T> results = new HashMap<Object, T>();
		for(T obj : coll){
			Object key = getIdValue(obj);
			results.put(key, obj);
		}
		return results;
	}

	private long executeCount() throws SQLException, NumberFormatException, IllegalArgumentException, NoSuchFieldException, IllegalAccessException, InstantiationException, NamingException, IOException, InvalidParameterException {
		long ct = 0;
		try {
		String sql = this.obj == null ? createSql(QueryType.count) : createSqlFromObj(QueryType.count);
		System.out.println("\nPQ QUERY: " + sql);
		DbTable tbl = cls.getAnnotation(DbTable.class);
		init(tbl.schema());
		prepareStatement(sql);
		setParameters(parms);
		ResultSet rs = executeQuery();
		if (rs.next()) {
			ct = rs.getLong("ct");
		}
		} finally {
			closeQuietly();
		}
		
		return ct;
	}
	private Collection<T> executeSelect() throws NamingException, SQLException, IOException, IllegalAccessException, InstantiationException, NoSuchFieldException, InvalidParameterException{
		Collection<T> coll = new ArrayList<T>();
		try {
		String sql = this.obj == null ? createSql(QueryType.select) : createSqlFromObj(QueryType.select);

		System.out.println("\nPQ QUERY: " + sql);
		DbTable tbl = cls.getAnnotation(DbTable.class);
		init(tbl.schema());
		prepareStatement(sql);
		setParameters(parms);
		ResultSet rs = executeQuery();
		Set<Field> fields = selectedFields;
		while(rs.next()){
			T obj = cls.newInstance();
			for(Field f : fields){
				DbColumn column = f.getAnnotation(DbColumn.class);
				if(column != null){
					Field obj_field = getField(obj.getClass(), f.getName());
					obj_field.setAccessible(true);
					Object value = ObjectConverter.convert(rs.getObject(column.value()), f.getType());
					obj_field.set(obj, value);
				}
			}
			for(String joinField : joinFields){
				join(obj, joinField);
			}
			coll.add(obj);
		}
		} finally {
			closeQuietly();
		}
		return coll;
	}
	private int executeUpdate(QueryType qt) throws NamingException, SQLException, NumberFormatException, IllegalArgumentException, NoSuchFieldException, IllegalAccessException, InstantiationException, IOException {
		int numUpdated = 0;
		try {
		String sql = origSql != null ? createSql(qt) : createSqlFromObj(qt);
		System.out.println("\nPQ QUERY: " + sql);
		DbTable tbl = cls.getAnnotation(DbTable.class);
		init(tbl.schema());
		// pass ids if qt is insert type so that we can retrieve autonumber/sequence ids with ps.getGeneratedKeys
		if (QueryType.insert == qt) {
			Set<Field> seqset = getFieldsWithSequence(cls);
			String[] seqcols = seqset.size() == 0 ? null : new String[seqset.size()];
			int idx = 0;
			for (Field f: seqset) {
				DbColumn col = f.getAnnotation(DbColumn.class);
				seqcols[idx++] = col.value();
			}
			prepareStatement(sql, seqcols);
			setParameters(parms);
			List<Object> keys = executeInsert();
			numUpdated = 1;
			idx = 0;
			for (Field f: seqset) {
				for (Object key: keys) {
					f.setAccessible(true);
					f.set(obj, ObjectConverter.convert(key, f.getType()));
				}
			}
		} else {
			prepareStatement(sql);
			setParameters(parms);
			numUpdated = executeUpdate();
		}
		} finally {
			closeQuietly();
		}
		return numUpdated;
	}
	
	public int update() throws SQLException, IOException, NamingException, NumberFormatException, IllegalArgumentException, NoSuchFieldException, IllegalAccessException, InstantiationException {
		Set<Field> dbfields = getFieldsWithAnnotation(DbColumn.class, this.cls);
		return update(dbfields);
	}
	public int update(String... fieldNamesToUpdate) throws SQLException, IOException, NamingException, SecurityException, NoSuchFieldException, NumberFormatException, IllegalArgumentException, IllegalAccessException, InstantiationException {
		Set<Field> fieldsToUpdate = new LinkedHashSet<Field>();
		for (String fn: fieldNamesToUpdate) {
			Field f = cls.getDeclaredField(fn);
			fieldsToUpdate.add(f);
		}
		return update(fieldsToUpdate);
	}
	public int updateIgnoreNulls() throws IllegalArgumentException, IllegalAccessException, SQLException, IOException, NamingException, NoSuchFieldException, InstantiationException {
		Set<Field> dbfields = getFieldsWithAnnotation(DbColumn.class, this.cls);
		Set<Field> fieldsToUpdate = new LinkedHashSet<Field>();
		for (Field f: dbfields) {
			f.setAccessible(true);
			Class<?> fcls = f.getType();
			Object val = f.get(obj);
			boolean nullprimitive = false;
			if(fcls.isPrimitive() && fcls != boolean.class){
				nullprimitive = ((Number) val).intValue() == 0;
			}
			if(!(nullprimitive || val == null)) {
				fieldsToUpdate.add(f);
			}
		}
		return update(fieldsToUpdate);
	}
	// actual update method
	public int update(Set<Field> fieldsToUpdate) throws NamingException, SQLException, NumberFormatException, IllegalArgumentException, NoSuchFieldException, IllegalAccessException, InstantiationException, IOException {
		int num_updated = 0;
		try {
		DbTable tbl = cls.getAnnotation(DbTable.class);
		String tblAlias = getTblAlias(cls);
		StringBuffer sql = new StringBuffer("update " + tbl.value() + " " + tblAlias + " set ");
		List<Object> updateParms = new ArrayList<Object>();
		Set<Field> updateableFields = new HashSet<Field>();
		for (Field f: fieldsToUpdate) {
			if (updateAllowed(f))
				updateableFields.add(f);
		}
		if (updateableFields.size() == 0)
			throw new IllegalAccessException("Update not allowed for user");
		// add update parameters 
		for (Field f: updateableFields) {
			DbColumn col = f.getAnnotation(DbColumn.class);
			sql.append(tblAlias + "." + col.value() + "=?,");
			f.setAccessible(true);
			updateParms.add(f.get(obj));
		}
		updateParms.addAll(parms);
		parms = updateParms;
		sql.setLength(sql.length()-1);
		// update by id if no where clause was passed
		if (this.origSql == null) {
			sql.append(" where ");
			Set<Field> idfields = getFieldsWithAnnotation(Id.class, this.cls);
			for (Field id: idfields) {
				id.setAccessible(true);
				DbColumn idcol = id.getAnnotation(DbColumn.class);
				sql.append(tblAlias + "." + idcol.value() + "=? and ");
				Object idval = id.get(this.obj);
				parms.add(idval);
			}
			sql.setLength(sql.length()-4);
		} 
		// update using the passed where clause and parms
		else {
			sql.append(" " + genJoinAndWhereStmts(cls, this.origSql));
		}
		System.out.println("PQ QUERY: " + sql.toString());
		init(tbl.schema());
		prepareStatement(sql.toString());
		setParameters(parms);
		num_updated = executeUpdate();
		} finally {
			closeQuietly();
		}
		return num_updated;
	}
	public int insert() throws NumberFormatException, IllegalArgumentException, NamingException, SQLException, NoSuchFieldException, IllegalAccessException, InstantiationException, IOException {
		return executeUpdate(QueryType.insert);
	}
	public int delete() throws NumberFormatException, IllegalArgumentException, NamingException, SQLException, NoSuchFieldException, IllegalAccessException, InstantiationException, IOException {
		return executeUpdate(QueryType.delete);
	}
	private String getTblAlias(Class<?> cls) throws NoSuchFieldException {
		DbTable tbl = cls.getAnnotation(DbTable.class);
		if (tbl == null)
			throw new NoSuchFieldException(cls.getSimpleName() + " does not have a @DbTable annotation");
		String tblalias = tbl.alias().equals("") ? cls.getSimpleName() : tbl.alias();
		return tblalias;
	}
	
	private String getAllJoinStmts() {
		StringBuffer joins = new StringBuffer();
		for (String js: joinstmts) {
			joins.append(js + " \n");
		}
		return joins.toString();
	}
	private String generateJoinStmt(Class<?> base, Class<?> joincls, JoinTable jt) throws SecurityException, NoSuchFieldException {
		StringBuffer sql = new StringBuffer();
		DbTable jointbl = joincls.getAnnotation(DbTable.class);
		String joinAlias = (jointbl.alias().length() > 0 ? jointbl.alias() : joincls.getSimpleName());
		String localAlias = getTblAlias(base);
		// check for a linking table
		if (jt.linkingTable().length > 0) {
			Class<?> linktblcls = jt.linkingTable()[0];
			DbTable linktbl = linktblcls.getAnnotation(DbTable.class);
			String linktblAlias = getTblAlias(linktblcls);
			sql.append("join " + linktbl.value() + " " + linktblAlias + " on ");
			for (int x=0; x<jt.localFields().length; x++) {
				DbColumn lcol = base.getDeclaredField(jt.localFields()[x]).getAnnotation(DbColumn.class);
				if (x > 0)
					sql.append(" and ");
				sql.append(localAlias + "." + lcol.value() + "=" + linktblAlias + "." + lcol.value());
			}
			
			sql.append("\njoin " + jointbl.value() + " " + joinAlias + " on ");
			for (int x=0; x<jt.localFields().length; x++) {
				DbColumn fcol = joincls.getDeclaredField(jt.foreignFields()[x]).getAnnotation(DbColumn.class);
				if (x > 0)
					sql.append(" and ");
				sql.append(linktblAlias + "." + fcol.value() + "=" + joinAlias + "." + fcol.value());
			}
			
		} 
		// no linking table exists
		else {
			sql.append("join " + jointbl.value() + " " + joinAlias + " on ");
			for (int x=0; x<jt.localFields().length; x++) {
				DbColumn lcol = base.getDeclaredField(jt.localFields()[x]).getAnnotation(DbColumn.class);
				DbColumn fcol = joincls.getDeclaredField(jt.foreignFields()[x]).getAnnotation(DbColumn.class);
				if (x > 0)
					sql.append(" and ");
				sql.append(localAlias + "." + lcol.value() + "=" + joinAlias + "." + fcol.value());
			}
		}
		return sql.toString();
	}
	
	private String genCountStmt() {
		//String tblalias = getTblAlias(cls);
		String sql = "select count(*) as ct";
		return sql;
	}
	private String genSelectStmt(String select) throws NoSuchFieldException, NumberFormatException, IllegalAccessException, InstantiationException, SQLException, NamingException, IOException {
		String tblalias = getTblAlias(cls);
		String selectAllRegex = "^\\s*select(\\s+[*]|" + tblalias + "\\.[*])?\\s*$";
		if (select == null || "".equals(select))
			select = "select *";
		if (select.matches(selectAllRegex)) {
			select = genSelectAllStmt();
		}
		else {
		Set<Field> fields = getAllFields(this.cls);
		for(Field f : fields){
			DbColumn col = f.getAnnotation(DbColumn.class);
			// check if the field is specified in the select query
			if(col != null){
				Matcher m = Pattern.compile("(" + tblalias + "\\.)?" + f.getName() + "([^(]|$)").matcher(select);
				//sql = sql.replaceAll(f.getName() + "([^(])", tblalias + "." + col.value() + "$1");
				while(m.find()) {
					selectedFields.add(f);
					select = select.substring(0, m.start()) + tblalias + "." + col.value() + m.group(2) + select.substring(m.end());
				}
			}
			// Check for jointable fields in the select statement
			JoinTable jt = f.getAnnotation(JoinTable.class);
			if(jt != null){
				// check the query for the presence of this join table column
				// if select contains a jointable field, it indicates that we must join to the table to retrieve the object
				if(select.indexOf(f.getName()) > -1){
					List<Integer[]> coords = new ArrayList<Integer[]>(); 
					Matcher m = Pattern.compile("(,|(\\s))+(" + f.getName() + "\\.?\\w*)").matcher(select);
					while(m.find()){
						joinFields.add(m.group(3));
						coords.add(new Integer[]{m.start(0),m.end(0)});
					}
					for(int i=coords.size()-1;i>=0;i--){
						select = select.substring(0,coords.get(i)[0]) + select.substring(coords.get(i)[1]);
					}
				}
			}
		}
		}
		
		if (select.matches(selectAllRegex))
			select = genSelectAllStmt();
		
		return select;
	}
	private String genSelectAllStmt() throws NoSuchFieldException, IllegalAccessException, NumberFormatException, InstantiationException, SQLException, NamingException, IOException {
		String tblalias = getTblAlias(cls);
		StringBuffer sb = new StringBuffer("select ");
		if (!securityEnabled) {
			sb.append(tblalias + ".* ");
			selectedFields = getAllFields(this.cls);
		} else {
			Set<Field> selectableFields = getSelectableFields(cls);
			if (selectableFields.size() == 0) {
				throw new IllegalAccessException("You don't have rights to select any data from " + tblalias + 
						". Please setup @PojoSecurity at the field level or ensure that the logged in user " + 
						//(securityUser ==null ? "" : "(" + securityUser.getCookieValue() + ") ") +
						"has rights to select data from this table.");
			}
			for (Field f: selectableFields) {
				DbColumn col = f.getAnnotation(DbColumn.class);
				sb.append(tblalias + "." + col.value() + ",");
				selectedFields.add(f);
			}
		}
		return sb.substring(0,sb.length()-1);
	}
	
	public String genFromStmt() throws NoSuchFieldException {
		DbTable db = cls.getAnnotation(DbTable.class);
		return "from " + db.value() + " " + getTblAlias(cls);
	}
	public String genJoinAndWhereStmts(Class<?> base, String query) throws NoSuchFieldException{
		Field[] fields = base.getDeclaredFields();
		String localAlias = getTblAlias(base);
		for (Field f: fields) {
			DbColumn col = f.getAnnotation(DbColumn.class);
			JoinTable jt = f.getAnnotation(JoinTable.class);
			if (jt != null) {
				//check joinquery for any of the fields that have JoinTable annotation
				String regexPattern = "\\s*(" + f.getName() + "\\.)([^ !=><)]+)";
				Matcher m = Pattern.compile(regexPattern).matcher(query);
				while (m.find()) {
					Class<?> joincls = getParameterizedType(f);
					String joinAlias = getTblAlias(joincls);
					String nxtField = m.group(2);
					// add to SET of joins
					joinstmts.add(generateJoinStmt(base, joincls, jt));
					// check if nxtField is a @DbColumn of joincls
					try {
					    Field joinf = joincls.getDeclaredField(nxtField);
					    DbColumn joincol = joinf.getAnnotation(DbColumn.class);
					    if (joincol != null) {
					    	whereReplacements.put(f.getName() + "\\." + joinf.getName(), joinAlias + "." + joincol.value());
					    }
					}
					catch ( NoSuchFieldException ex) {
					    // field doesn't exist; rerun generateJoinStmt with joincls
						query = query.substring(0, m.start(1)) + query.substring(m.end(1));
						genJoinAndWhereStmts(joincls, query);
					}			
				}
			} else if (col != null) {
				List<Integer[]> coords = new ArrayList<Integer[]>(); 
				String regexPattern = "\\s*(" + f.getName() + ")([ !=<>)]+|$)";
				Matcher m = Pattern.compile(regexPattern).matcher(query);
				while (m.find()) {
					coords.add(new Integer[]{m.start(1),m.end(1)});
				}
				for(int i=coords.size()-1;i>=0;i--){
					query = query.substring(0,coords.get(i)[0])  + localAlias + "." + col.value() + query.substring(coords.get(i)[1]);
				}
			}
		}
		
		for (String regex: whereReplacements.keySet()) {
			query = query.replaceAll(regex, whereReplacements.get(regex));
		}
		return getAllJoinStmts() +  query;
	}
	
	private String genOrderBy(String clause) throws NoSuchFieldException {
		String tblalias = getTblAlias(cls);
		Set<Field> fields = getAllFields(this.cls);
		for(Field f : fields){
			DbColumn col = f.getAnnotation(DbColumn.class);
			// check if the field is specified in the select query
			if(col != null){
				Matcher m = Pattern.compile("(" + tblalias + "\\.)?" + f.getName() + "([, ]|$)").matcher(clause);
				//sql = sql.replaceAll(f.getName() + "([^(])", tblalias + "." + col.value() + "$1");
				while(m.find()) {
					//selectedFields.add(f);
					clause = clause.substring(0, m.start()) + tblalias + "." + col.value() + m.group(2) + clause.substring(m.end());
				}
			}
		}
		return clause;
	}
	private Class<?> getParameterizedType(Field f){
		Class<?> parameterized_type = null;
		boolean is_collection = Collection.class.isAssignableFrom(f.getType());
		boolean is_map = Map.class.isAssignableFrom(f.getType());
		// check to see if Field is a Collection
		if(is_collection){
			ParameterizedType parameterizedType = (ParameterizedType) f.getGenericType();
			parameterized_type = (Class<?>) parameterizedType.getActualTypeArguments()[0];
		}
		else if(is_map){
			ParameterizedType parameterizedType = (ParameterizedType) f.getGenericType();
			parameterized_type = (Class<?>) parameterizedType.getActualTypeArguments()[1];
		}
		// Just a single Object exists... must be one to one relation
		else{
			parameterized_type = f.getType();
		}
		return parameterized_type;
	}
	public String getSql(QueryType qt) throws NumberFormatException, NoSuchFieldException, IllegalAccessException, InstantiationException, SQLException, NamingException, IOException{
		String sql = origSql != null ? createSql(qt) : createSqlFromObj(qt);
		return sql;
	}

	private static final Set<Field> getAllFields(Class<?> currcls){
		Set<Field> fields = new LinkedHashSet<Field>();
		Field[] farr = currcls.getDeclaredFields();
		for(int x = 0; x < farr.length; x++){
			fields.add(farr[x]);
		}
		// add fields from super class if they exist
		while(currcls.getSuperclass() != null){
			currcls = currcls.getSuperclass();
			farr = currcls.getDeclaredFields();
			for(int x = 0; x < farr.length; x++){
				fields.add(farr[x]);
			}
		}
		return fields;
	}
	private static final <A extends Annotation>Set<Field> getFieldsWithAnnotation(Class<A> annotation, Class<?> cls){
		Set<Field> fieldset = new LinkedHashSet<Field>();
		Set<Field> fields = getAllFields(cls);
		for(Field f : fields){
			if(f.getAnnotation(annotation) != null){
				fieldset.add(f);
			}
		}
		return fieldset;
	}
	private static final Set<Field> getFieldsWithSequence(Class<?> cls) {
		Set<Field> seqset = new LinkedHashSet<Field>();
		Set<Field> idset = getFieldsWithAnnotation(Id.class, cls);
		for (Field id: idset) {
			if (!"".equals(id.getAnnotation(Id.class).sequence()))
				seqset.add(id);
		}
		return seqset;
	}
	private Set<Field> getSelectableFields(Class<?> cls) throws NumberFormatException, IllegalAccessException, InstantiationException, SQLException, NamingException, IOException, NoSuchFieldException {
		Set<Field> fieldset = new LinkedHashSet<Field>();
		Field[] fields = cls.getDeclaredFields();
		for (Field f: fields) {
			if (selectAllowed(f)) {
				if (!Byte[].class.isAssignableFrom(f.getType()))
					fieldset.add(f);
				else if (retrieveBlobs)
					fieldset.add(f);
			}
		}
		
		return fieldset;
	}
	private Field getField(Class<?> cls, String fieldName) throws NoSuchFieldException{
		try{
			return cls.getDeclaredField(fieldName);
		}
		catch(NoSuchFieldException e){
			Class<?> superClass = cls.getSuperclass();
			if(superClass == null){
				throw e;
			}
			else{
				return getField(superClass, fieldName);
			}
		}
	}
	private boolean isFieldInitialized(Field f, Object val) {
		Class<?> t = f.getType();
		if (boolean.class.equals(t) && Boolean.FALSE.equals(val)) 
			return false;
		else if (char.class.equals(t) && ((Character) val) != Character.MIN_VALUE)
			return false;
		else if (t.isPrimitive() && ((Number) val).doubleValue() == 0)
			return false;
		else if(!t.isPrimitive() && val == null)
			return false;
		else
			return val != null;
	}
	private boolean insertAllowed() throws IllegalAccessException, InstantiationException, SQLException, NamingException, IOException, NoSuchFieldException {
		if (!securityEnabled)
			return true;
		else {
			String security = getClassSecurity();
			return security.matches("^1\\d$");
		}
	}
	private boolean deleteAllowed() throws IllegalAccessException, InstantiationException, SQLException, NamingException, IOException, NoSuchFieldException {
		if (!securityEnabled)
			return true;
		else {
			String security = getClassSecurity();
			return security.matches("^\\d1$");
		}
	}
	private boolean updateAllowed(Field f) throws NumberFormatException, IllegalAccessException, InstantiationException, SQLException, NamingException, IOException, NoSuchFieldException {
		if (!securityEnabled)
			return true;
		else {
			String security = getFieldSecurity(f);
			return security.matches("^\\d1\\d$");
		}
	}
	private boolean insertFieldAllowed(Field f) throws NumberFormatException, IllegalAccessException, InstantiationException, SQLException, NamingException, IOException, NoSuchFieldException {
		if (!securityEnabled)
			return true;
		else {
			String security = getFieldSecurity(f);
			return security.matches("^\\d\\d1$");
		}
	}
	private boolean selectAllowed(Field f) throws NumberFormatException, IllegalAccessException, InstantiationException, SQLException, NamingException, IOException, NoSuchFieldException {
		if (!securityEnabled)
			return true;
		else {
			String security = getFieldSecurity(f);
			return security.matches("^1\\d\\d$");
		}
	}
	private String getFieldSecurity(Field f) throws NumberFormatException, IllegalAccessException, InstantiationException, SQLException, NamingException, IOException, NoSuchFieldException {
		int gssu = 0;
		int essu = 0;
		// setup field security (select, update, insert)
		PojoSecurity pjs = f.getAnnotation(PojoSecurity.class);
		if(pjs != null){
			if(securityUser != null){
				for(int x = 0; x < pjs.groups().length; x++){
					if(securityUser.isInGroup(pjs.groups()[x])){
						gssu = gssu | Integer.parseInt(pjs.groupsecurity()[x], 2);
					}
				}
			}
			essu = Integer.parseInt(pjs.everyone(), 2);
		}
		String retstr = String.format("%03d", Integer.parseInt(Integer.toString(gssu | essu, 2)));
		return retstr;
	}
	private String getClassSecurity() throws IllegalAccessException, InstantiationException, SQLException, NamingException, IOException, NoSuchFieldException{
		int gsid = 0;
		int esid = 0;
		PojoSecurity tblsec = cls.getAnnotation(PojoSecurity.class);
		// setup insert/delete rights based on GroupSecurity and TblSessionSecurity annotations
		if(tblsec != null){
			// Insert-Delete
			for(int x = 0; x < tblsec.groups().length; x++){
				if(securityUser != null && securityUser.isInGroup(tblsec.groups()[x])){
					gsid = gsid | Integer.parseInt(tblsec.groupsecurity()[x], 2);
				}
			}
			esid = Integer.parseInt(tblsec.everyone(), 2);
			// if (securityUser != null)
			// ssid = ssid | Integer.parseInt(tblss.owner(),2);
		}
		return String.format("%02d", Integer.parseInt(Integer.toString(gsid | esid, 2)));
	}
	
	public Set<DBField> getFieldObjects(PqUser u) throws IllegalAccessException, InstantiationException, SQLException, NamingException, IOException, NoSuchFieldException{
		Set<DBField> fields = new HashSet<DBField>();
		Field[] pfields = cls.getDeclaredFields();
		for(Field f : pfields){
			DBField result = getFieldObject(u, f.getName());
			if(result != null){
				fields.add(result);
			}
		}
		return fields;
	}
	public Set<DBField> getSelectableFieldObjects(PqUser u) throws NumberFormatException, IllegalAccessException, InstantiationException, SQLException, NamingException, IOException, NoSuchFieldException {
		Set<DBField> fields = new HashSet<DBField>();
		Set<Field> sfields = getSelectableFields(cls);
		for(Field f: sfields) {
			DBField result = getFieldObject(u, f.getName());
			if(result != null){
				fields.add(result);
			}
		}
		return fields;
	}
	public Set<DBField> getIdFieldObjects(PqUser u) throws NumberFormatException, SecurityException, NoSuchFieldException, IllegalAccessException, InstantiationException, SQLException, NamingException, IOException {
		Set<DBField> fields = new HashSet<DBField>();
		Set<Field> sfields = getFieldsWithAnnotation(Id.class, cls);
		for(Field f: sfields) {
			DBField result = getFieldObject(u, f.getName());
			if(result != null){
				fields.add(result);
			}
		}
		return fields;
	}
	
	public DBField getFieldObject(PqUser u, String fname) throws SecurityException, NoSuchFieldException, NumberFormatException, IllegalAccessException, InstantiationException, SQLException, NamingException, IOException{
		if(securityUser == null){
			securityUser = u;
		}
		String localfname = fname;
		String foreignfname = null;
		String[] tmp = fname.split("\\.");
		if(tmp.length > 1){
			localfname = tmp[0];
			foreignfname = tmp[1];
		}
		DBField retField = new DBField();

		Field f = cls.getDeclaredField(localfname);
		String type = f.getType().getSimpleName();

		// check for foreign field with fname
		if(foreignfname != null){
			
			Class<?> cls = getParameterizedType(f);
			f = cls.getDeclaredField(foreignfname);
		}
		JoinTable jt = f.getAnnotation(JoinTable.class);
		if(jt != null){
			type = "join";
			if(jt.linkingTable().length > 0){
				retField.hasLinkingTable(true);
			}
			retField.setLocalLinkids(jt.localFields());
			retField.setForeignLinkids(jt.foreignFields());
		}
		
		// setup field level select/update rights based on GroupSecurity and SessionSecurity annotations
		// setup fields
		retField.setType(type);
		retField.setFieldName(f.getName());
		retField.setId(f.getAnnotation(Id.class) != null);
		retField.setDdlJSON(null);
		// merge session security and group security (return whichever one has more rights if both are defined)
		// String rights = Integer.toString((Integer.parseInt(sessionSecurity,4) | Integer.parseInt(groupSecurity,4)), 4).replaceAll("3", "1");
		String rights = String.format("%05d", 100 * Integer.parseInt(getFieldSecurity(f)) + Integer.parseInt(getClassSecurity()));
		retField.setSecurity(rights);
		return retField;
	}


	public static final Map<String, Object> getIdValue(Object obj) throws IllegalArgumentException, IllegalAccessException{
		Map<String,Object> ids = new HashMap<String,Object>();
		Set<Field> idfields = getFieldsWithAnnotation(Id.class, obj.getClass());
		for(Field f : idfields){
			f.setAccessible(true);
			Object val = f.get(obj);
			if(f.getType() == String.class){
				val = "\"" + val + "\"";
			}
			else if(f.getType() == java.util.Date.class){
				val = ((java.util.Date) val).getTime();
			}
			if (val == null)
				return null;
			ids.put(f.getName(), val);
		}
		return ids;
	}
	protected PojoQuery<T> addLinkingTblJoin(JoinTable jt) throws NoSuchFieldException {
		StringBuffer join = new StringBuffer("join ");
		Class<?> linkTblCls = jt.linkingTable()[0];
		DbTable linkTbl = linkTblCls.getAnnotation(DbTable.class);
		String linkTblAlias = getTblAlias(linkTblCls);
		String joinTblAlias = getTblAlias(this.cls);
		join.append(linkTbl.value() + " " + linkTblAlias +" on "); 
		for (int x=0; x<jt.foreignFields().length; x++) {
			DbColumn col = this.cls.getDeclaredField(jt.foreignFields()[x]).getAnnotation(DbColumn.class);
			if (x > 0)
				join.append(" and ");
			join.append(linkTblAlias + "." + col.value() + "=" + joinTblAlias + "." + col.value());
		}
		joinstmts.add(join.toString());
		return this;
	}
	public <X>void join(X obj, String joinField) throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException, InstantiationException, NamingException, SQLException, IOException, InvalidParameterException{
		if (obj == null || joinField == null)
			return;
		String[] jfs = joinField.split("\\.");
		String cjoin = jfs[0];
		// perform join
		String nxtjoin = "";
		for(int x = 1; x < jfs.length; x++){
			if(x != 1){
				nxtjoin += ".";
			}
			nxtjoin += jfs[x];
		}
		// get field of 'obj' that we will be joining to.
		Field jfield = obj.getClass().getDeclaredField(cjoin);
		jfield.setAccessible(true);
		// get JoinTable annotation and related data from jfield
		JoinTable jt = jfield.getAnnotation(JoinTable.class);
		Class<?> joinctype = jfield.getType();
		Class<X> joincls = (Class<X>) getParameterizedType(jfield);
		// check if nxtjoin is a field of joincls
		Field jclsfield = null;
		try {
			jclsfield = joincls.getDeclaredField(nxtjoin);
		} catch (NoSuchFieldException e) { 
			//empty catch. indicates no field found 
		}
		String select = jclsfield != null ? "select " + jclsfield.getName() : "";
		StringBuffer where = new StringBuffer(" where ");
		Object[] parms = new Object[jt.localFields().length];
		// check if there is a linking table we have to go through in order to get the data from the foreign table
		if (jt.linkingTable().length > 0) {
			Class<?> linkTblCls = jt.linkingTable()[0];
			String linkTblAlias = getTblAlias(linkTblCls);
			for (int x=0; x<jt.localFields().length; x++) {
				DbColumn linkTblCol = linkTblCls.getDeclaredField(jt.localFields()[x]).getAnnotation(DbColumn.class);
				if (x!=0)
					where.append(" and ");
				where.append(linkTblAlias + "." + linkTblCol.value() + "=?");
				Field lfield = obj.getClass().getDeclaredField(jt.localFields()[x]);
				lfield.setAccessible(true);
				parms[x] = lfield.get(obj);
			}
		}
		else {
			for (int x=0; x<jt.foreignFields().length; x++) {
				if (x!=0)
					where.append(" and ");
				where.append(jt.foreignFields()[x] + "=?");
				Field lfield = obj.getClass().getDeclaredField(jt.localFields()[x]);
				lfield.setAccessible(true);
				parms[x] = lfield.get(obj);
			}
		}
		String jsql = (select + where).trim();
		PojoQuery<X> jpq = new PojoQuery<X>(joincls, jsql, parms);
		// add in linking table join to the query 
		if (jt.linkingTable().length > 0) {
			jpq.addLinkingTblJoin(jt);
		}
		// setup security for the join
		if (!securityEnabled)
			jpq.disableSecurity();
		else
			jpq.setSecurityUser(securityUser);
		if(List.class.isAssignableFrom(joinctype)){
			List<X> list = jpq.list();
			jfield.set(obj, list);
			if (!"".equals(nxtjoin) && jclsfield == null) {
				for (X jobj: list) {
					join(jobj, nxtjoin);
				}
			}
		}
		else if(Map.class.isAssignableFrom(joinctype)){
			Map<?, X> map = jpq.map();
			jfield.set(obj, map);
			if (!"".equals(nxtjoin) && jclsfield == null) {
				for (X jobj: map.values()) {
					join(jobj, nxtjoin);
				}
			}
		}
		else if(Set.class.isAssignableFrom(joinctype)){
			Set<X> set = jpq.set();
			jfield.set(obj, set);
			if (!"".equals(nxtjoin) && jclsfield == null) {
				for (X jobj: set) {
					join(jobj, nxtjoin);
				}
			}
		}
		else{
			X jobj = jpq.single();
			jfield.set(obj, jobj);
			if (!"".equals(nxtjoin) && jclsfield == null) {
				join(jobj, nxtjoin);
			}
		}
	}
}
