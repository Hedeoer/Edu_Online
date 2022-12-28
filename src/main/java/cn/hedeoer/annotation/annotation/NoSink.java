package cn.hedeoer.annotation.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * ClassName: NoSink
 * Package: cn.hedeoer.exec.annotation
 * Description:
 *
 * @Author hedeoer
 * @Create 2022/12/14 16:17
 */

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface NoSink {

}
