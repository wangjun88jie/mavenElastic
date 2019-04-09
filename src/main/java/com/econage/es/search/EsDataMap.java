package com.econage.es.search;

import com.econage.es.exception.ElasticException;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class EsDataMap extends HashMap<String,Object> {
    private String esId;
    private String updateType;//   index 新增； upsert如果不存在新增，如果存在更新 ； update更新
    public EsDataMap(String esId,String updateType){
        super();
        this.esId = esId;
        this.updateType = updateType;
    }
    public EsDataMap(){

    }

    public EsDataMap(String esId,String updateType,Map<String,Object> data){
        super(data);
        this.esId = esId;
        this.updateType = updateType;
    }

    public String getEsId() throws ElasticException {
        if(StringUtils.isEmpty(esId)){
            throw new ElasticException("ES_ID is Empty");
        }
        return esId;
    }

    public void setEsId(String esId) {
        this.esId = esId;
    }

    public String getUpdateType() {
        if(StringUtils.isEmpty(updateType)){
            return "index";
        }
        return updateType;
    }

    public void setUpdateType(String updateType) {
        this.updateType = updateType;
    }

    @Override
    public boolean equals(Object o) {
        if(null!=o && esId.equals(((EsDataMap)o).esId)){
            return true;
        }
        return false;
    }
}
