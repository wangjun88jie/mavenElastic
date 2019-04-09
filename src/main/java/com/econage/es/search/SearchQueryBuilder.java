package com.econage.es.search;

import com.econage.es.search.dao.EsColumnEntity;
import org.apache.commons.lang3.ArrayUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;

import java.util.Map;

/**
 * 条件查询
 */
public class SearchQueryBuilder extends BoolQueryBuilder {
    public static final char UNDERLINE = '_';
    private Map<String, EsColumnEntity> fieldMap;
    private Map<String, EsColumnEntity> esColMap;
    public static SearchQueryBuilder createQueryBuilder(Map<String, EsColumnEntity> fieldMap, Map<String, EsColumnEntity> esColMap){
        return new SearchQueryBuilder(fieldMap,esColMap);
    }

    public static SearchQueryBuilder createQueryBuilder(){
        return new SearchQueryBuilder();
    }

    private SearchQueryBuilder(Map<String, EsColumnEntity> fieldMap, Map<String, EsColumnEntity> esColMap){
        this.fieldMap = fieldMap;
        this.esColMap = esColMap;
    }

    private SearchQueryBuilder(){
    }

    /**
     * 关键字查询
     * @param queryBuilder
     * @param value
     * @param keyWordColumnArr
     * @return
     */
    public SearchQueryBuilder keyWordSearch(SearchQueryBuilder queryBuilder,String value,String... keyWordColumnArr){
        if(ArrayUtils.isNotEmpty(keyWordColumnArr)){
            String[] esColArr = new String[keyWordColumnArr.length];
            int i=0;
            for(String keyWordColumn : keyWordColumnArr){
                esColArr[i++] = getEsColName(keyWordColumn);
            }
            return (SearchQueryBuilder)queryBuilder.must(QueryBuilders.multiMatchQuery(value,esColArr));
        }
        return (SearchQueryBuilder)queryBuilder.must(QueryBuilders.multiMatchQuery(value,keyWordColumnArr));
    }

    /**
     * 全匹配查询
     * @param queryBuilder
     * @param value
     * @param esCol
     * @return
     */
    public SearchQueryBuilder termsSearchForm(SearchQueryBuilder queryBuilder,String esCol,String... value){
        return (SearchQueryBuilder)queryBuilder.must(QueryBuilders.termsQuery(getEsColName(esCol),value));
    }



    /**
     * 区间查询
     * @param queryBuilder
     * @param esCol
     * @param valueS
     * @param valueE
     * @return
     */
    public SearchQueryBuilder rangeSearch(SearchQueryBuilder queryBuilder,String esCol,Object valueS,Object valueE){
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(getEsColName(esCol));
        if(valueS!=null){
            rangeQueryBuilder.from(valueS);
        }
        if(valueE!=null){
            rangeQueryBuilder.to(valueE);
        }
        return (SearchQueryBuilder)queryBuilder.must(rangeQueryBuilder);
    }


    /**
     * 将搜索字段转成es里的字段
     * @param col
     * @return
     */
    private String getEsColName(String col){
        if(esColMap.get(col)!=null){
            return col;
        }
        if(fieldMap.get(col)!=null && !Strings.isNullOrEmpty(fieldMap.get(col).getColumn())){
            return fieldMap.get(col).getColumn();
        }
        return EsUtils.camelToUnderline(col);
    }





}
