package com.econage.es;

import com.econage.es.pool.CommonVar;
import com.econage.es.search.searchEnum.EsColumnType;

import java.lang.annotation.*;

@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EsIndex_ {
    //索引
    String es_index();
    //类型
    String es_type() default "";
    //如果索引不存在 是否创建索引和Mapping
    boolean is_create() default false;
    int shards_() default CommonVar.ES_SHARDS;
    int replicas_() default CommonVar.ES_REPLICAS;



}
