package com.econage.es.pool;

import com.econage.es.exception.ElasticInitException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;


public class TransportClientFactory extends BasePooledObjectFactory<TransportClient> {
    private static Logger logger = Logger.getLogger(TransportClientFactory.class);
    private String[] hosts;
    private String[] ports;
    private String clusterName;
    private int index = 0;
    //其他参数待定

    public void init(String[] hosts,String[] ports,String clusterName) throws ElasticInitException {
        this.hosts = hosts;
        this.ports = ports;
        this.clusterName = clusterName;
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

    }

    @Override
    public TransportClient create() throws Exception {


        Settings settings = Settings.builder().put("cluster.name", clusterName)
                .put("client.transport.sniff",true)
                .build();

        TransportClient client = new PreBuiltTransportClient(settings);

        for(int i=0;i<hosts.length;i++){
            client.addTransportAddresses(new TransportAddress(InetAddress.getByName(hosts[i]), Integer.parseInt(ports[i])));
        }

        return client;
    }


    @Override
    public void destroyObject(PooledObject<TransportClient> p) throws Exception {
        System.out.println("---------------destroyObject-----------------");
        TransportClient client = p.getObject();
        client.close();
    }




    @Override
    public PooledObject<TransportClient> wrap(TransportClient transportClient) {
        return new DefaultPooledObject(transportClient);
    }

    @Override
    public boolean validateObject(PooledObject<TransportClient> p) {
        try {
            TransportClient client = p.getObject();
            if (CollectionUtils.isNotEmpty(client.listedNodes())){
                return true;
            }
            return false;
        } catch (Throwable t) {
            return false;
        }
    }

    public void passivateObject(PooledObject<TransportClient> p) throws Exception {
        if(p instanceof TransportClient){
            ((TransportClient)p).close();
        }
    }
}
