package com.econage.es.search.service;

import com.econage.es.configure.ConfigureUtils;
import com.econage.es.exception.ElasticException;
import com.econage.es.pool.ClientService;
import com.econage.es.pool.CommonVar;
import com.econage.es.search.AbstractEsClient;
import com.econage.es.search.EsDataMap;
import com.econage.es.search.EsUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;
import org.apache.poi.ss.formula.functions.T;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * 索引数据新增改查的service
 */
public class EsDataService extends AbstractEsClient {
    private static EsDataService ourInstance = new EsDataService();
    private static Logger logger = Logger.getLogger(EsDataService.class);
    private static String logPrefix = ConfigureUtils.configureEntity.getLogInfo();
    public static EsDataService getInstance() {
        return ourInstance;
    }

    private EsDataService() {
    }

    /**********************************************ES数据的增删改查***************************************************/
    /**
     * 批量更新或者插入
     * @param index
     * @param type
     * @param updateType
     * @param data
     */
    public  void createDocBulk(final String index, final String type, final String updateType, final List<EsDataMap> data) throws ElasticException {
        logger.info(logPrefix+"create Bulk docs");
        logger.info(logPrefix+"index:【"+index+"】");
        logger.info(logPrefix+"type:【"+type+"】");
        logger.info(logPrefix+"updateType:【"+updateType+"】");
        if (CollectionUtils.isEmpty(data)) {
            return ;
        }
        runTransaction(new TransactionAction() {
            @Override
            protected T execute(TransportClient client) throws ElasticException {

                try {
                    //建立批量提交类
                    BulkRequestBuilder bulkRequest = client.prepareBulk();
                    XContentBuilder xContentBuilder;
                    for (EsDataMap map : data){
                        if(MapUtils.isEmpty(map)){
                            continue;
                        }
                        xContentBuilder = jsonBuilder().startObject();
                        for(String key:map.keySet()){
                            logger.info(logPrefix+"key:"+key+"_____value:"+map.get(key));
                            xContentBuilder.field(key,EsUtils.fillValue(key,map));
                        }
                        xContentBuilder.endObject();
                        EsUtils.setPrepare(client,index,type,map.getEsId(),xContentBuilder,updateType);
                    }
                    //批量提交到服务器
                    BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                    if (bulkResponse.hasFailures()) {
                        //服务器返回错误
                        throw new ElasticException(bulkResponse.buildFailureMessage());
                    }
                    logger.info(logPrefix+"create Bulk docs success");
                }catch (Exception e){
                    e.printStackTrace();
                }
                return null;
            }
        });
    }



    /**
     * 部分字段   批量更新
     * @param index   索引名
     * @param type    索引类型
     * @param data    更新数据
     * @param columnArr createDocBulk  指定更新的字段
     * @throws ElasticException
     */
    protected void updateDocBulkByColumn(final String index, final String type, final List<EsDataMap> data, final String[] columnArr) throws ElasticException {
        //todo 部分字段更新
        logger.info(logPrefix+"update Bulk docs By Column");
        logger.info(logPrefix+"index:【"+index+"】");
        logger.info(logPrefix+"type:【"+type+"】");
        if(ArrayUtils.isEmpty(columnArr)){
            throw new ElasticException(logPrefix+"update column is empty");
        }
        if (CollectionUtils.isEmpty(data)) {
            return;
        }
        runTransaction(new TransactionAction() {
            @Override
            protected T execute(TransportClient client) throws ElasticException {
                try {
                    //建立批量提交类
                    BulkRequestBuilder bulkRequest = client.prepareBulk();
                    XContentBuilder xContentBuilder;
                    for (EsDataMap map : data){
                        if(MapUtils.isEmpty(map)){
                            continue;
                        }
                        xContentBuilder = jsonBuilder().startObject();
                        for(String key:columnArr){
                            logger.info(logPrefix+"key:"+key+"_____value:"+map.get(key));

                            xContentBuilder.field(key,EsUtils.fillValue(key,map));
                        }
                        xContentBuilder.endObject();
                        bulkRequest.add(client.prepareUpdate(index, type, map.getEsId())
                                .setDoc(xContentBuilder));
                    }
                    //批量提交到服务器
                    BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                    if (bulkResponse.hasFailures()) {
                        //服务器返回错误
                        throw new ElasticException(bulkResponse.buildFailureMessage());
                    }
                    logger.info(logPrefix+"create Bulk docs success");
                }catch (Exception e){
                    e.printStackTrace();
                }
                return null;
            }
        });


    }


    /**
     * 单条插入ES
     * @param index
     * @param type
     * @param updateType
     * @param map
     */
    public  void createDocSingle(final String index, final String type, final String updateType, final EsDataMap map) throws ElasticException {
        if(MapUtils.isEmpty(map)){
            return ;
        }
        logger.info(logPrefix+"create single doc");
        logger.info(logPrefix+"index:【"+index+"】");
        logger.info(logPrefix+"type:【"+type+"】");
        logger.info(logPrefix+"updateType:【"+updateType+"】");

        runTransaction(new TransactionAction() {
            @Override
            protected T execute(TransportClient client) throws ElasticException {
                try {
                    XContentBuilder xContentBuilder = jsonBuilder().startObject();
                    for (String key : map.keySet()) {
                        logger.info(logPrefix + "key:" + key + "_____value:" + map.get(key));
                        if (key.equals("ES_ID")) {
                            //如果是ES的Id跳过
                            continue;
                        }
                        xContentBuilder.field(key, EsUtils.fillValue(key,map));
                    }
                    xContentBuilder.endObject();
                    xContentBuilder.flush();
                    xContentBuilder.close();
                    EsUtils.setPrepare(client,index,type,map.getEsId(),xContentBuilder,updateType);

                    logger.info(logPrefix + "create single doc success");
                }catch (IOException e){
                    new ElasticException("",e);
                }
                return null;
            }
        });


    }


    /**
     * 单条 部分字段更新
     * @param index
     * @param type
     * @param data
     * @param columnArr
     */
    public void updateDocSingleByColumn(final String index, final String type, final EsDataMap data, final String[] columnArr) throws ElasticException {
        if(MapUtils.isEmpty(data)){
            return ;
        }
        logger.info(logPrefix+"create single doc");
        logger.info(logPrefix+"index:【"+index+"】");
        logger.info(logPrefix+"type:【"+type+"】");

        if(ArrayUtils.isEmpty(columnArr)){
            throw new ElasticException(logPrefix+"update column is empty");
        }
        runTransaction(new TransactionAction() {
            @Override
            protected T execute(TransportClient client) throws ElasticException {
                try {
                    XContentBuilder xContentBuilder = jsonBuilder().startObject();
                    for (String key : columnArr) {
                        logger.info(logPrefix + "key:" + key + "_____value:" + data.get(key));
                        if (key.equals("ES_ID")) {
                            //如果是ES的Id跳过
                            continue;
                        }
                        xContentBuilder.field(key, EsUtils.fillValue(key,data));
                    }
                    xContentBuilder.endObject();
                    xContentBuilder.flush();
                    xContentBuilder.close();
                    client.prepareUpdate(index, type, data.getEsId()).
                            setDoc(xContentBuilder).get();


                    logger.info(logPrefix + "create single doc success");
                }catch (IOException e){
                    new ElasticException("",e);
                }

                return null;
            }
        });


    }


    /**
     * 校验数据是否存在
     * @param index
     * @param type
     * @param esId
     * @return
     * @throws ElasticException
     */
    public boolean existsData(final String index, final String type, final String esId) throws ElasticException {
        return runTransactionBoolean(new TransactionActionBoolean() {

            @Override
            protected boolean executeBoolean(TransportClient client) throws ElasticException {
                GetRequest getRequest = new GetRequest(
                        index,
                        type,
                        esId);
                getRequest.fetchSourceContext(new FetchSourceContext(false));//禁用抓取_source
                getRequest.storedFields("_none_");//禁用提取存储的字段。
                ActionFuture<GetResponse> getResponse = client.get(getRequest);
                GetResponse response = getResponse.actionGet();
               return response.isExists();
            }
        });
    }


    /**
     * 删除数据
     * @param index
     * @param type
     * @param id
     * @throws ElasticException
     */
    public void  deleteById(final String index, final String type, final String id) throws ElasticException {

        runTransactionBoolean(new TransactionActionBoolean() {
            @Override
            protected boolean executeBoolean(TransportClient client) throws ElasticException {

                DeleteResponse result = client.prepareDelete().setIndex(index)
                        .setType(type)
                        .setId(id)//设置ID
                        .execute().actionGet();
                //是否查找并删除
                logger.info(logPrefix+result.toString());
                return false;
            }
        });
    }


    public static void main(String[] args) throws ElasticException {
        ClientService.getInstance().testServiceCofig(null);
        //EsDataService.getInstance().existsData("filedata3","order","222");
        EsDataService.getInstance().deleteById("test_index","test_type",
                "1");
    }

}
