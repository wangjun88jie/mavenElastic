package com.econage.es.pool;
import com.econage.es.configure.ConfigureEntity;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

public class ClientService {
    private static Logger logger = Logger.getLogger(ClientService.class);
    private static ClientService ourInstance = new ClientService();

    public static ClientService getInstance() {
        return ourInstance;
    }
    private GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
    TransportClientFactory factory = new TransportClientFactory();
    private static GenericObjectPool<TransportClient> pool;
    private ClientService() {

    }
    public void setCinfig(ConfigureEntity configureEntity){
        /*logger.info(CommonVar.lOG_INFO+"ES和连接池配置:");
        logger.info(CommonVar.lOG_INFO+"最多创建资源数:"+config.get(CommonVar.ES_MAX_ACTIVE));
        logger.info(CommonVar.lOG_INFO+"最少资源空闲数:"+config.get(CommonVar.ES_INITIAL_SIZE));*/
        poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(configureEntity.getMaxTotal());//最多创建资源数
        poolConfig.setMaxIdle(configureEntity.getMaxIdle());//最大空闲数
        poolConfig.setMinIdle(configureEntity.getMinIdle());//最少资源空闲数

        poolConfig.setTestOnBorrow(true);//borrow时 校验
        factory.init(configureEntity.getHost(),configureEntity.getPort(),configureEntity.getClusterName());
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

    public void testServiceCofig(ConfigureEntity entity){
        Map<String,String> mapCofig = new HashMap<String,String>();
        ClientService.getInstance().setCinfig(entity);
    }


    public static void main(String[] args) throws Exception {
        //ClientService.getInstance().getConnection();
    }

}
