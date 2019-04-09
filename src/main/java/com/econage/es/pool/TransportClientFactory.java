package com.econage.es.pool;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;


public class TransportClientFactory extends BasePooledObjectFactory<TransportClient> {
    //private static Logger logger = Logger.getLogger(TransportClientFactory.class);
    private String host;
    private int port;
    private String clusterName;
    //其他参数待定

    public void init(String host,int port,String clusterName){
        this.host = host;
        this.port = port;
        this.clusterName = clusterName;
    }

    @Override
    public TransportClient create() throws Exception {
       /* logger.info(CommonVar.lOG_INFO+clusterName);
        logger.info(CommonVar.lOG_INFO+host);
        logger.info(CommonVar.lOG_INFO+port);*/
        Settings settings = Settings.builder().put("cluster.name", clusterName).build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddresses(new TransportAddress(InetAddress.getByName(host), port));
        return client;
    }


    @Override
    public void destroyObject(PooledObject<TransportClient> p) throws Exception {
        if(p instanceof TransportClient){
            ((TransportClient)p).close();
        }
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
}
