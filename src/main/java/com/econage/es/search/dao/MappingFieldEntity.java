package com.econage.es.search.dao;

import com.econage.es.search.searchEnum.EsColumnType;

/**
 * mapping字段相关属性
 * 字段名,字段类型,分析器,format
 */
public class MappingFieldEntity {
    private String name;
    private EsColumnType type;
    private String analyzer;
    private String searchAnalyzer;
    private String[] format;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public EsColumnType getType() {
        return type;
    }

    public void setType(EsColumnType type) {
        this.type = type;
    }

    public String getAnalyzer() {
        return analyzer;
    }

    public void setAnalyzer(String analyzer) {
        this.analyzer = analyzer;
    }

    public String getSearchAnalyzer() {
        return searchAnalyzer;
    }

    public void setSearchAnalyzer(String searchAnalyzer) {
        this.searchAnalyzer = searchAnalyzer;
    }

    public String[] getFormat() {
        return format;
    }

    public void setFormat(String[] format) {
        this.format = format;
    }
}
