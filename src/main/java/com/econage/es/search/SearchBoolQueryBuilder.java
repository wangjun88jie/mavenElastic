package com.econage.es.search;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;

import java.util.List;

public class SearchBoolQueryBuilder extends BoolQueryBuilder {

    public SearchBoolQueryBuilder(){

    }
    /*public static SearchBoolQueryBuilder createQueryBuilder(){
        return new SearchBoolQueryBuilder();
    }*/

    public static SearchBoolQueryBuilder mustBuilders(List<SearchQueryBuilder> queryBuilders){
        SearchBoolQueryBuilder searchBoolQueryBuilder = new SearchBoolQueryBuilder();
        if(CollectionUtils.isNotEmpty(queryBuilders)){
            for(SearchQueryBuilder searchQueryBuilder : queryBuilders){
                searchBoolQueryBuilder.must(searchQueryBuilder);
            }
        }
        return searchBoolQueryBuilder;
    }

    public static SearchBoolQueryBuilder shouldBuilders(List<SearchQueryBuilder> queryBuilders){
        SearchBoolQueryBuilder searchBoolQueryBuilder = new SearchBoolQueryBuilder();
        if(CollectionUtils.isNotEmpty(queryBuilders)){
            for(SearchQueryBuilder searchQueryBuilder : queryBuilders){
                searchBoolQueryBuilder.should(searchQueryBuilder);
            }
        }
        return searchBoolQueryBuilder;
    }

}
