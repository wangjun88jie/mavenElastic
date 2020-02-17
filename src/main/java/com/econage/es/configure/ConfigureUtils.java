package com.econage.es.configure;
import com.econage.es.exception.ElasticInitException;
import com.econage.es.pool.ClientService;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
/**
 * 读取配置工具类
 */
public class ConfigureUtils{
    private static PropertiesConfiguration cfg = null;
    public static ConfigureEntity configureEntity = null;
    static {
        configureEntity = new ConfigureEntity();
        try {
            cfg = new PropertiesConfiguration("elastic.properties");
        } catch (ConfigurationException e){
            e.printStackTrace();
        }
        // 当文件的内容发生改变时，配置对象也会刷新
        cfg.setReloadingStrategy(new FileChangedReloadingStrategy());
        configureEntity.setMaxTotal(getIntValue("maxTotal")>0?getIntValue("maxTotal"):20);
        configureEntity.setMaxActive(getIntValue("maxActive")>0?getIntValue("maxActive"):10);
        configureEntity.setMaxIdle(getIntValue("maxIdle")>0?getIntValue("maxIdle"):10);
        configureEntity.setMinIdle(getIntValue("minIdle")>0?getIntValue("minIdle"):3);
        if(StringUtils.isNotEmpty(getStringValue("hosts").trim())){
            configureEntity.setHosts(getStringValue("hosts").trim().split(","));
        }

        if(StringUtils.isNotEmpty(getStringValue("ports").trim())){
            configureEntity.setPorts(getStringValue("ports").trim().split(","));
        }


        configureEntity.setClusterName(getStringValue("clusterName").trim());
        configureEntity.setLogInfo(getStringValue("logInfo").trim());
        configureEntity.setDefaultDateFormat(StringUtils.isNotEmpty(getStringValue("defaultDateFormat"))?
                getStringValue("defaultDateFormat").trim():"yyyy-MM-dd hh:mm:ss");
        configureEntity.setShards(getIntValue("shards"));
        configureEntity.setReplicas(getIntValue("replicas"));
        configureEntity.setScheme(StringUtils.isNotEmpty(getStringValue("scheme"))?
                getStringValue("scheme").trim():"http");

        //启动对象池
        try {
            ClientService.getInstance().testServiceCofig(configureEntity);
        } catch (ElasticInitException e) {
            e.printStackTrace();
        }
    }

    public static String getStringValue(String key){
        return cfg.getString(key);
    }
    // 读int
    public static int getIntValue(String key){
        return cfg.getInt(key);
    }
    // 读boolean
    public static boolean getBooleanValue(String key){
        return cfg.getBoolean(key);
    }
    // 读List
    public static List<?> getListValue(String key){
        return cfg.getList(key);
    }
    // 读数组
    public static String[] getArrayValue(String key){
        return cfg.getStringArray(key);
    }
}
