package dbutil.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/* value if t
 * 
 * 
 * */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface UserId{
	boolean AdUser() default false;
	boolean setIdOnInsert() default false;
	boolean setIdOnUpdate() default false;
}