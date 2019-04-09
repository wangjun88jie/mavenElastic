package com.econage.es.search.searchEnum;

/**
 * ES所具有的类型,其中TEXT es会分词，而KEYWORD不会分词
 */
public enum EsColumnType {
    TEXT,KEYWORD,DATE,LONG,INTEGER,SHORT,BYTE,DOUBLE,FLOAT,
    BINARY
}
