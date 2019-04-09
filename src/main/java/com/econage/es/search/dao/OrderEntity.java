package com.econage.es.search.dao;

import com.econage.es.search.searchEnum.EsColumnType;
import org.elasticsearch.search.sort.SortOrder;

public class OrderEntity {
    private String name;
    private EsColumnType type;
    private SortOrder desc;

    public OrderEntity(){}

    public OrderEntity(String name,EsColumnType type,SortOrder desc){
        this.name = name;
        this.type = type;
        this.desc = desc;
    }
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

    public SortOrder getDesc() {
        return desc;
    }

    public void setDesc(SortOrder desc) {
        this.desc = desc;
    }

    public static OrderEntity create(String name, EsColumnType type, SortOrder desc){
        return new OrderEntity(name,type,desc);
    }

}
