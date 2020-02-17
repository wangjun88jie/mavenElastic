package com.econage.es.pool;
import com.econage.es.configure.ConfigureEntity;
import com.econage.es.exception.ElasticInitException;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.log4j.Logger;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.HashMap;
import java.util.Map;

public class RestHighClientService {
    private static Logger logger = Logger.getLogger(ClientService.class);
    private static RestHighClientService ourInstance = new RestHighClientService();

    public static RestHighClientService getInstance() {
        return ourInstance;
    }
    private GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
    RestHighClientFactory restHighClientFactory = new RestHighClientFactory();
    //RestHighClientFactory
    private static GenericObjectPool<RestHighLevelClient> pool;
    private RestHighClientService() {

    }
    public void setConfig(ConfigureEntity configureEntity) throws ElasticInitException {
        /*logger.info(CommonVar.lOG_INFO+"ES和连接池配置:");
        logger.info(CommonVar.lOG_INFO+"最多创建资源数:"+config.get(CommonVar.ES_MAX_ACTIVE));
        logger.info(CommonVar.lOG_INFO+"最少资源空闲数:"+config.get(CommonVar.ES_INITIAL_SIZE));*/
        poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(configureEntity.getMaxTotal());//最多创建资源数
        poolConfig.setMaxIdle(configureEntity.getMaxIdle());//最大空闲数
        poolConfig.setMinIdle(configureEntity.getMinIdle());//最少资源空闲数

        poolConfig.setTestOnBorrow(true);//borrow时 校验
        restHighClientFactory.init(configureEntity.getHosts(),configureEntity.getPorts(),configureEntity.getClusterName(),configureEntity.getScheme());

        poolConfig.setTestOnBorrow(true);
        pool = new GenericObjectPool(restHighClientFactory, poolConfig);

    }
    public RestHighLevelClient getClient() {
        RestHighLevelClient client = null;
        try {
            client = pool.borrowObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }




    public void returnObject(RestHighLevelClient client){
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

    public void testServiceCofig(ConfigureEntity entity) throws ElasticInitException {
        Map<String,String> mapCofig = new HashMap<String,String>();
        RestHighClientService.getInstance().setConfig(entity);
    }


    public static void main(String[] args) throws Exception {
        //ClientService.getInstance().getConnection();
    }

}
