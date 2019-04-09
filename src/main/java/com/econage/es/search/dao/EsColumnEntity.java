package com.econage.es.search.dao;

import com.econage.es.search.searchEnum.EsColumnType;

public class EsColumnEntity {
    private String name;//字段名称
    private String column;//es库字段名称
    private EsColumnType esType;//es字段数据类型
    private String type;//实体类数据类型
    private String format;//格式
    private boolean interval;//是否区间字段
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public EsColumnType getEsType() {
        return esType;
    }

    public void setEsType(EsColumnType esType) {
        this.esType = esType;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public boolean isInterval() {
        return interval;
    }

    public void setInterval(boolean interval) {
        this.interval = interval;
    }

}
