package com.econage.es.search.dao;

/**
 * ES的Setting
 */
public class SettingsEntity {
    private String index;
    private int shards;//主分片数
    private int replicas;//副分片数

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public int getShards() {
        return shards;
    }

    public void setShards(int shards) {
        this.shards = shards;
    }

    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }
}
