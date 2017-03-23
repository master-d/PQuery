package dbutil.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Define Select,Update,Insert,Delete Rights for a PojoDatabase class <br/> When used at the class level <br/> &lt;security&gt; = "&lt;insert&gt;&lt;delete&gt;"<br/> when used at the field level<br/> security = "&ltselect&gt;&ltupdate&gt;&lt;insert&gt;"
 */
@Target({ElementType.FIELD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface PojoSecurity{
	// priority [owner,group,everyone] higher number means it takes precendence
	// int[] priority() default {2,1,0};
	String[] groups() default {};
	/**
	 * When used at the class level <br/> &lt;security&gt; = "&lt;insert&gt;&lt;delete&gt;"<br/> when used at the field level<br/> security = "&ltselect&gt;&ltupdate&gt;&lt;insert&gt;"
	 */
	String[] groupsecurity() default {};
	/**
	 * When used at the class level <br/> &lt;security&gt; = "&lt;insert&gt;&lt;delete&gt;"<br/> when used at the field level<br/> security = "&ltselect&gt;&ltupdate&gt;&lt;insert&gt;"
	 */
	String everyone() default "000";
	/**
	 * When used at the class level <br/> &lt;security&gt; = "&lt;insert&gt;&lt;delete&gt;"<br/> when used at the field level<br/> security = "&ltselect&gt;&ltupdate&gt;&lt;insert&gt;"
	 */
	String owner() default "100";
}