package dbutil;

import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Generic object converter. <p> <h3>Use examples</h3> <pre> Object o1 = Boolean.TRUE; Integer i = ObjectConverter.convert(o1, Integer.class); System.out.println(i); // 1 Object o2 = "false"; Boolean b = ObjectConverter.convert(o2, Boolean.class); System.out.println(b); // false Object o3 = new Integer(123); String s = ObjectConverter.convert(o3, String.class); System.out.println(s); // 123 </pre> Not all possible conversions are implemented. You can extend the <tt>ObjectConverter</tt> easily by just adding a new method to it, with the appropriate logic. For example: <pre> public static ToObject fromObjectToObject(FromObject fromObject) { // Implement. } </pre> The method name doesn't matter. It's all about the parameter type and the return type.
 * @author BalusC
 * @link http://balusc.blogspot.com/2007/08/generic-object-converter.html
 */
public final class ObjectConverter{
	// Init ---------------------------------------------------------------------------------------
	private static final Map<String, Method> CONVERTERS = new HashMap<String, Method>();
	static{
		// Preload converters.
		Method[] methods = ObjectConverter.class.getDeclaredMethods();
		for(Method method : methods){
			if(method.getParameterTypes().length == 1){
				// Converter should accept 1 argument. This skips the convert() method.
				//System.out.println("adding converter " +method.getParameterTypes()[0].getSimpleName() + "_"	+ method.getReturnType().getSimpleName());
				CONVERTERS.put((method.getParameterTypes()[0].getSimpleName() + "_"	+ method.getReturnType().getSimpleName()).toLowerCase(), method);
			}
		}
	}
	private ObjectConverter(){
		// Utility class, hide the constructor.
	}
	
	/**
	 * Convert the given object value to the given class.
	 * @param from The object value to be converted.
	 * @param to The type class which the given object should be converted to.
	 * @return The converted object value.
	 * @throws NullPointerException If 'to' is null.
	 * @throws UnsupportedOperationException If no suitable converter can be found.
	 * @throws RuntimeException If conversion failed somehow. This can be caused by at least an ExceptionInInitializerError, IllegalAccessException or InvocationTargetException.
	 */
	public static <T> T convert(Object from, Class<T> to){
		// check to see if to is a primitive
		if(to.isPrimitive()){
			if(to.getSimpleName().equals("int")){
				convert(from,Integer.class);
			}
			else if(to.getSimpleName().equals("boolean")){
				convert(from,Boolean.class);
			}
			else if(to.getSimpleName().equals("double")){
				convert(from,Double.class);
			}
			else if(to.getSimpleName().equals("long")){
				convert(from,Long.class);
			}
			else if(to.getSimpleName().equals("char")){
				convert(from,Character.class);
			}
			else if(to.getSimpleName().equals("float")){
				convert(from,Float.class);
			}
			else if (to.getSimpleName().equals("short")) {
				convert(from,Short.class);
			}
				
		}
		
		// Null is just null.
		if(from == null){
			return null;
		}
		// Can we cast? Then just do it.
		if(to.isAssignableFrom(from.getClass())){
			return to.cast(from);
		}
		// Lookup the suitable converter.
		String converterId = from.getClass().getSimpleName() + "_" + to.getSimpleName();
		// System.out.println("Looking for converter.... " + converterId);
		Method converter = CONVERTERS.get(converterId.toLowerCase());
		if(converter == null){
			throw new UnsupportedOperationException("Cannot convert from "
					+ from.getClass().getName() + " to " + to.getName()
					+ ". Requested converter does not exist.");
		}
		// Convert the value.
		try{
			return to.cast(converter.invoke(to, from));
		}
		catch(Exception e){
			throw new RuntimeException("Cannot convert from "
					+ from.getClass().getName() + " to " + to.getName()
					+ ". Conversion failed with " + e.getMessage(), e);
		}
		
	}
	// Converters ---------------------------------------------------------------------------------
	/**
	 * Converts Integer to Boolean. If integer value is 0, then return FALSE, else return TRUE.
	 * @param value The Integer to be converted.
	 * @return The converted Boolean value.
	 */
	public static Boolean integerToBoolean(Integer value){
		return value.intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
	}
	/**
	 * Converts Boolean to Integer. If boolean value is TRUE, then return 1, else return 0.
	 * @param value The Boolean to be converted.
	 * @return The converted Integer value.
	 */
	public static Integer booleanToInteger(Boolean value){
		return value.booleanValue() ? Integer.valueOf(1) : Integer.valueOf(0);
	}
	/**
	 * Converts Double to BigDecimal.
	 * @param value The Double to be converted.
	 * @return The converted BigDecimal value.
	 */
	public static BigDecimal doubleToBigDecimal(Double value){
		return new BigDecimal(value.doubleValue());
	}
	public static BigDecimal integerToBigDecimal(Integer value){
		return new BigDecimal(value.intValue());
	}
	public static BigDecimal longToBigDecimal(Long value){
		return new BigDecimal(value.longValue());
	}
	/**
	 * Converts BigDecimal to Double.
	 * @param value The BigDecimal to be converted.
	 * @return The converted Double value.
	 */
	public static Double bigDecimalToDouble(BigDecimal value){
		return new Double(value.doubleValue());
	}
	public static Integer bigDecimalToInteger(BigDecimal value){
		return Integer.valueOf(value.intValue());
	}
	public static Long bigDecimalToLong(BigDecimal value){
		return Long.valueOf(value.longValue());
	}
	public static Integer shortToInteger(java.lang.Short value) {
		return Integer.valueOf(value);
	}
	/**
	 * Converts Integer to String.
	 * @param value The Integer to be converted.
	 * @return The converted String value.
	 */
	public static String integerToString(Integer value){
		return value.toString();
	}
	/**
	 * Converts String to Integer.
	 * @param value The String to be converted.
	 * @return The converted Integer value.
	 */
	public static Integer stringToInteger(String value){
		return Integer.valueOf(value);
	}
	/**
	 * Converts Boolean to String.
	 * @param value The Boolean to be converted.
	 * @return The converted String value.
	 */
	public static String booleanToString(Boolean value){
		return value.toString();
	}
	/**
	 * Converts String to Boolean.
	 * @param value The String to be converted.
	 * @return The converted Boolean value.
	 */
	public static Boolean stringToBoolean(String value){
		return value.equalsIgnoreCase("y");
	}
	/**
	 * Converts CLOB to String.
	 * @param value The CLOB to be converted.
	 * @return The converted String value.
	 * @throws SQLException  
	 * @throws IOException 
	 */
	public static String CLOBToString(java.sql.Clob clob) throws IOException, SQLException {
		if (clob == null)
			return null;
		else
			return new String(DbUtility.inputStreamToByteArray(clob.getAsciiStream()));
	}
	
	public static byte[] blobToByteArray(Blob b) throws SQLException, IOException {
		//b.get
		//byte[] bytes = b.getBytes(1, (int)b.length());
		byte[] bytes = DbUtility.inputStreamToByteArray(b.getBinaryStream());
		b.free();
		return bytes;
	}
}