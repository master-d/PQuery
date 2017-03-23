package dbutil;

import java.util.HashMap;

public class DBField {
	
	String fieldName;
	String foreignFieldName;
	String type;
	boolean id;
	String ddlJSON;
	String security;
	boolean linkingTable;
	HashMap<String, String[]> linkids;
	
	public DBField() { }

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public String getForeignFieldName() {
		return foreignFieldName;
	}

	public void setForeignFieldName(String foreignFieldName) {
		this.foreignFieldName = foreignFieldName;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public boolean isId() {
		return id;
	}

	public void setId(boolean id) {
		this.id = id;
	}

	public String getDdlJSON() {
		return ddlJSON;
	}

	public void setDdlJSON(String ddlJSON) {
		this.ddlJSON = ddlJSON;
	}

	public String getSecurity() {
		return security;
	}

	public void setSecurity(String security) {
		this.security = security;
	}

	public void hasLinkingTable(boolean val) {
		this.linkingTable = val;
	}

	public boolean hasLinkingTable() {
		return linkingTable;
	}

	public HashMap<String, String[]> getLinkids() {
		return linkids;
	}

	public void setLocalLinkids(String[] cols) {
		if (linkids == null)
			linkids = new HashMap<String, String[]>();
		linkids.put("local", cols);
	}
	public void setForeignLinkids(String[] cols) {
		if (linkids == null)
			linkids = new HashMap<String, String[]>();
		linkids.put("foreign", cols);
	}
	public void setLinkids(HashMap<String, String[]> linkids) {
		this.linkids = linkids;
	}
	
	@Override
	public int hashCode() {
		String combined = fieldName + foreignFieldName;
		return combined.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
	      if (this == obj)
	          return true;
	       if (obj == null)
	          return false;
	       if (getClass() != obj.getClass())
	          return false;
	       DBField other = (DBField) obj;
	       if (foreignFieldName == null && other.getForeignFieldName() == null) {
	    	   return fieldName.equals(other.getFieldName());
	       } else if (foreignFieldName != null && other.getForeignFieldName() != null) {
	    	   return fieldName.equals(other.getFieldName()) && foreignFieldName.equals(other.getForeignFieldName()); 
	       } else
	    	   return false;
	}
}
