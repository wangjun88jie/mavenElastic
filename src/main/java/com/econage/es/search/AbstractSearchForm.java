package com.econage.es.search;

import com.econage.es.search.dao.OrderEntity;

import java.util.Collection;
import java.util.List;

public abstract class AbstractSearchForm {

    private String[] sort;//转换成ES的枚举类
    private List<OrderEntity> orders;//排序相关
    private int page;
    private int rows;


    public String[] getSort() {
        return sort;
    }

    public void setSort(String[] sort) {
        this.sort = sort;
    }

    public List<OrderEntity> getOrders() {
        return orders;
    }

    public void setOrders(List<OrderEntity> orders) {
        this.orders = orders;
    }

    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public int getRows() {
        return rows;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }
}
