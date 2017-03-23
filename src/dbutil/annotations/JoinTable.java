package dbutil.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface JoinTable{
	// Annotation element definitions
	String[] localFields();
	String[] foreignFields();
	String[] localLinkingColumns() default "";
	String[] foreignLinkingColumns() default "";
	String joinConditions() default "";
	// LinkingTable linkingTable() default @LinkingTable(value = "");
	Class<?>[] linkingTable() default {};
}