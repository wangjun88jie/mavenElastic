package com.econage.es.pool;

import com.econage.es.exception.ElasticInitException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.client.*;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;


public class RestHighClientFactory extends BasePooledObjectFactory<RestHighLevelClient> {
    private static Logger logger = Logger.getLogger(RestHighClientFactory.class);
    private String[] hosts;
    private String[] ports;
    private String clusterName;
    private String scheme;
    private List<HttpHost> httpHostList;
    //其他参数待定

    public void init(String[] hosts,String[] ports,String clusterName,String scheme) throws ElasticInitException {

        this.hosts = hosts;
        this.ports = ports;
        this.clusterName = clusterName;
        this.scheme = scheme;
        if(StringUtils.isEmpty(clusterName)){
            logger.error(CommonVar.lOG_INFO+"未配置参数【clusterName】");
            throw new ElasticInitException(CommonVar.lOG_INFO+"未配置参数【clusterName】");
        }
        if(ArrayUtils.isEmpty(hosts)){
            logger.error(CommonVar.lOG_INFO+"未配置参数【hosts】");
            throw new ElasticInitException(CommonVar.lOG_INFO+"未配置参数【hosts】");
        }
        if(ArrayUtils.isEmpty(ports)){
            logger.error(CommonVar.lOG_INFO+"未配置参数【ports】");
            throw new ElasticInitException(CommonVar.lOG_INFO+"未配置参数【ports】");
        }

        logger.info(CommonVar.lOG_INFO+"cluster:"+clusterName);
        logger.info(CommonVar.lOG_INFO+"hosts:"+StringUtils.join(hosts,","));
        logger.info(CommonVar.lOG_INFO+"ports:"+StringUtils.join(ports,","));

        httpHostList = new ArrayList<>();
        for(int i=0;i<hosts.length;i++){
            httpHostList.add(new HttpHost(hosts[i], Integer.parseInt(ports[i]), scheme));
        }

    }

    @Override
    public RestHighLevelClient create() throws Exception {



        RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(httpHostList.toArray(new HttpHost[]{})));

        return client;
    }


    @Override
    public void destroyObject(PooledObject<RestHighLevelClient> p) throws Exception {
        if(p instanceof RestHighLevelClient){
            ((RestHighLevelClient)p).close();
        }
    }




    @Override
    public PooledObject<RestHighLevelClient> wrap(RestHighLevelClient client) {
        return new DefaultPooledObject(client);
    }

    @Override
    public boolean validateObject(PooledObject<RestHighLevelClient > p) {
        try {
            RestHighLevelClient  client = p.getObject();
           if (true){
                return true;
            }
            return false;
        } catch (Throwable t) {
            return false;
        }
    }
}
