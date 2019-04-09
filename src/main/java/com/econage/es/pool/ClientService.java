package com.econage.es.pool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

public class ClientService {
    //private static Logger logger = Logger.getLogger(ClientService.class);
    private static ClientService ourInstance = new ClientService();

    public static ClientService getInstance() {
        return ourInstance;
    }
    private GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
    TransportClientFactory factory = new TransportClientFactory();
    private static GenericObjectPool<TransportClient> pool;
    private ClientService() {

    }
    public void setCinfig(Map<String,String> config){
        /*logger.info(CommonVar.lOG_INFO+"ES和连接池配置:");
        logger.info(CommonVar.lOG_INFO+"最多创建资源数:"+config.get(CommonVar.ES_MAX_ACTIVE));
        logger.info(CommonVar.lOG_INFO+"最少资源空闲数:"+config.get(CommonVar.ES_INITIAL_SIZE));*/
        poolConfig = new GenericObjectPoolConfig();

        poolConfig.setMaxTotal(Integer.parseInt(config.get(CommonVar.ES_MAX_ACTIVE)));//最多创建资源数
        poolConfig.setMinIdle(Integer.parseInt(config.get(CommonVar.ES_INITIAL_SIZE)));//最少资源空闲数
        poolConfig.setMaxIdle(10);//最大空闲数
        poolConfig.setTestOnBorrow(true);//borrow时 校验
        factory.init(config.get(CommonVar.ES_HOST),Integer.parseInt(config.get(CommonVar.ES_PORT)),config.get(CommonVar.ES_CLUSTER_NAME));
        poolConfig.setTestOnBorrow(true);
        pool = new GenericObjectPool(factory, poolConfig);

    }
    public TransportClient getClient() {
        TransportClient client = null;
        try {
            client = pool.borrowObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }


    public void returnObject(TransportClient client){
        try{
            pool.returnObject(client);
        }catch(Exception e){
            e.printStackTrace();
            if(client != null){
                try{
                    client.close();
                }catch(Exception ex){
                    ex.printStackTrace();
                }
            }
        }
    }

    public void testServiceCofig(){
        Map<String,String> mapCofig = new HashMap<String,String>();
        mapCofig.put(CommonVar.ES_HOST,"192.168.0.70");
        mapCofig.put(CommonVar.ES_PORT,String.valueOf(9300));
        mapCofig.put(CommonVar.ES_CLUSTER_NAME,"elasticsearch");
        mapCofig.put(CommonVar.ES_MAX_ACTIVE,String.valueOf(10));
        mapCofig.put(CommonVar.ES_INITIAL_SIZE,String.valueOf(3));
        ClientService.getInstance().setCinfig(mapCofig);
    }


    public static void main(String[] args) throws Exception {
        //ClientService.getInstance().getConnection();
    }

}
