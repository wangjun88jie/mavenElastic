package com.econage.es.dao;

import com.econage.es.EsColumnProperty_;
import com.econage.es.EsId_;
import com.econage.es.EsIndex_;
import com.econage.es.search.searchEnum.EsColumnType;

import java.util.Date;

@EsIndex_(es_index = "test_index",is_create = true,es_type = "test_type")
public class TestEntity {
    @EsColumnProperty_(column="CREATE_USER",type = EsColumnType.TEXT)
    private String name;
    @EsColumnProperty_(column="CONTENT",type = EsColumnType.TEXT)
    private String content;
    @EsColumnProperty_(column="CREATE_DATE",type = EsColumnType.DATE,format = "yyyy-MM-dd hh:mm:ss")
    private Date date;
    @EsColumnProperty_(column="ID",type = EsColumnType.INTEGER)
    @EsId_
    private int id;
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static TestEntity create(int id,String name,String content,Date date){
        TestEntity entity = new TestEntity();
        entity.setId(id);
        entity.setName(name);
        entity.setContent(content);
        entity.setDate(date);
        return entity;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }
}
