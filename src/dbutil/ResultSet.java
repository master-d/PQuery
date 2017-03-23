package dbutil;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.util.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.security.InvalidParameterException;

public class ResultSet{
	private Queue<Map<String,Object>> queue = new LinkedList<Map<String,Object>>();
	private boolean goNext = false;

	
	protected void push(Map<String,Object> map) {
		queue.add(map);
	}
	public boolean next(){
		if(goNext && !queue.isEmpty()){
			queue.remove();
		}else{
			goNext = true;
		}
		return !queue.isEmpty();
	}
	public int getSize() {
		return queue.size();
	}
	public Map<String, Object> getRow(){
		return queue.peek();
	}
	public <T> T get(String param,Class<T> t) throws InvalidParameterException{
		return ObjectConverter.convert(getObject(param),t);
	}
	public Object getObject(String param) throws InvalidParameterException{
		param = param.toUpperCase();
		if(!queue.element().containsKey(param)){
			throw new InvalidParameterException("Invalid column name: " + param);
		}else{
			return queue.element().get(param);
		}
	}
	public String getString(String param) throws InvalidParameterException{
		return get(param,String.class);
	}
	public Integer getInt(String param) throws InvalidParameterException{
		return get(param,Integer.class);
	}
	public Double getDouble(String param) throws InvalidParameterException{
		return get(param,Double.class);
	}
	public Long getLong(String param) throws InvalidParameterException{
		return get(param,Long.class);
	}
	public Short getShort(String param) throws InvalidParameterException{
		return get(param,Short.class);
	}
	public Float getFloat(String param) throws InvalidParameterException{
		return get(param,Float.class);
	}
	public Blob getBlob(String param) throws InvalidParameterException{
		return get(param,Blob.class);
	}
	public Clob getClob(String param) throws InvalidParameterException{
		return get(param,Clob.class);
	}
	public byte[] getBytes(String param) throws InvalidParameterException{
		return get(param,byte[].class);
	}
	public BigDecimal getBigDecimal(String param) throws InvalidParameterException{
		return get(param,BigDecimal.class);
	}
	public Date getDate(String param) throws InvalidParameterException{
		return get(param,Date.class);
	}
	public Time getTime(String param) throws InvalidParameterException{
		return get(param,Time.class);
	}
	public Timestamp getTimeStamp(String param) throws InvalidParameterException{
		return get(param,Timestamp.class);
	}
}
