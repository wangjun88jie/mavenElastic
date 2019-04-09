package com.econage.es;

import com.econage.es.search.searchEnum.EsColumnType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface EsColumnProperty_ {
    //ES里的字段
    String column();
    //ES里的类型
    EsColumnType type() default EsColumnType.TEXT;
    //ES里的时间format
    String format() default "";

}
