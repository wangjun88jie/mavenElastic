package com.econage.es.search.service;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.econage.es.configure.ConfigureUtils;
import com.econage.es.exception.ElasticException;
import com.econage.es.pool.ClientService;
import com.econage.es.pool.CommonVar;
import com.econage.es.search.AbstractEsClient;
import com.econage.es.search.dao.EsColumnEntity;
import com.econage.es.search.dao.MappingFieldEntity;
import com.econage.es.search.dao.SettingsEntity;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.poi.ss.formula.functions.T;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * 索引的操作
 */
public class EsIndexService extends AbstractEsClient {
    private static Logger logger = Logger.getLogger(EsIndexService.class);
    private static EsIndexService ourInstance = new EsIndexService();

    public static EsIndexService getInstance() {
        return ourInstance;
    }

    private EsIndexService() {
        logger.info(ConfigureUtils.configureEntity.getLogInfo()+"instantiation");
    }


    /**********************************************ES服务器的一些操作方法*/
    /**
     * 创建索引
     * @param indexName  索引名称
     */
    public void createIndex(final String indexName, final Settings settings) throws ElasticException {
         runTransaction(new TransactionAction() {
            @Override
            protected T execute(TransportClient client) throws ElasticException {
                client.admin().
                        indices().prepareCreate(indexName).setSettings(settings).get();
                return null;
            }
        });

    }

    /**
     * 创建索引
     * @param indexName  索引名称
     */
    public void createIndex(final String indexName, final int shards, final int replicas) throws ElasticException {
        runTransaction(new TransactionAction() {
            @Override
            protected T execute(TransportClient client) throws ElasticException {
                client.admin().
                        indices().prepareCreate(indexName).setSettings( createSettingsBuilder(shards,replicas)).get();
                return null;
            }
        });

    }

    /**
     * 删除索引
     * @param indexName 索引名称
     */
    public void deleteIndex(final String indexName) throws ElasticException {

        runTransaction(new TransactionAction() {
            @Override
            protected T execute(TransportClient client) throws ElasticException {
                DeleteIndexResponse response = ClientService.getInstance().getClient().admin().indices().prepareDelete(indexName).get();
                logger.info(ConfigureUtils.configureEntity.getLogInfo()+"delete index success 【"+indexName+"】");
                //todo 返回值 待定
                return null;
            }
        });
    }

    /**
     * 判断索引是否存在
     * @param index
     * @return
     */
    public boolean indexExists(final String index) throws ElasticException {
        return runTransactionBoolean(new TransactionActionBoolean() {
            @Override
            protected boolean executeBoolean(TransportClient client) throws ElasticException {
                IndicesExistsRequest request = new IndicesExistsRequest(index);
                IndicesExistsResponse response = client.admin().indices().exists(request).actionGet();
                if (response.isExists()){
                    return true;
                }
                return false;
            }
        });
    }

    /**
     * 刷新索引
     * @param indexs
     */
    public boolean refreshIndex(final String... indexs) throws Exception {
        return runTransactionBoolean(new TransactionActionBoolean() {
            @Override
            protected boolean executeBoolean(TransportClient client) throws ElasticException {
                client.admin().indices().prepareRefresh(indexs).get();
                return true;
            }
        });
    }

    /**
     * 刷新全部索引
     */
    public boolean refreshAllIndex() throws Exception {
        return runTransactionBoolean(new TransactionActionBoolean() {
            @Override
            protected boolean executeBoolean(TransportClient client) throws ElasticException {
                client.admin().indices().prepareRefresh().get();
                return true;
            }
        });
    }

    /**
     * 获取索引的 Setting
     * @param index
     * @return
     */
    public SettingsEntity getSetting(final String index) throws Exception {
        final SettingsEntity settingsEntity = new SettingsEntity();
        runTransaction(new TransactionAction() {
            @Override
            protected T execute(TransportClient client) throws ElasticException {
                GetSettingsResponse response = client.admin().indices()
                        .prepareGetSettings(index).get();
                for (ObjectObjectCursor<String, Settings> cursor : response.getIndexToSettings()) {
                    if(cursor.key.equals(index)){
                        Settings settings = cursor.value;
                        Integer shards = settings.getAsInt("index.number_of_shards", null);
                        Integer replicas = settings.getAsInt("index.number_of_replicas", null);
                        settingsEntity.setIndex(cursor.key);
                        settingsEntity.setShards(shards);
                        settingsEntity.setReplicas(replicas);
                        break;
                    }
                }
                return null;
            }
        });
        return settingsEntity;
    }




    /**
     * 设置Settings属性 目前只了解 shard数和replica数
     * @param shardsNum
     * @param replicasNum
     * @return
     */
    public Settings.Builder createSettingsBuilder(int shardsNum,int replicasNum){
        Settings.Builder sb = Settings.builder()
                .put("index.number_of_shards", shardsNum)
                .put("index.number_of_replicas", replicasNum);
        return sb;
    }

    /**
     * 设置Settings属性 目前只了解 shard数和replica数
     * @param settingsEntity
     * @return
     */
    public Settings.Builder createSettingsBuilder(SettingsEntity settingsEntity){
        if(settingsEntity!=null){
            Settings.Builder sb = Settings.builder()
                    .put("index.number_of_shards", settingsEntity.getShards())
                    .put("index.number_of_replicas", settingsEntity.getReplicas());
            return sb;
        }
        return null;
    }


    /**
     * 更新es里的mapping
     * @param index
     * @param type
     * @param mapping
     */
    public void putMapping(final String index, final String type, final XContentBuilder mapping) throws ElasticException {
        if(!indexExists(index)){
            throw new ElasticException("index【"+index+"】,no exists");
        }
        runTransaction(new TransactionAction() {
            @Override
            protected T execute(TransportClient client) throws ElasticException {
                try {
                    PutMappingRequest mappingRequest = Requests.putMappingRequest(index).type(type).source(mapping);
                   client.admin().indices().putMapping(mappingRequest).get();
                    logger.info(ConfigureUtils.configureEntity.getLogInfo()+"create index:【"+index+"】 type:【"+type+"】Mapping success");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new ElasticException("",e);
                } catch (ExecutionException e) {
                    throw new ElasticException("",e);
                }

                return null;
            }
        });
    }


    /**
     * 创建mapping对象
     * @param mappingFieldList
     * @return
     */
    public XContentBuilder createMapping(List<MappingFieldEntity> mappingFieldList){
        XContentBuilder mapping = null;
        if(CollectionUtils.isEmpty(mappingFieldList)){
            return mapping;
        }
        try{
            mapping = XContentFactory.jsonBuilder().startObject();
            mapping.startObject("properties");
            for(MappingFieldEntity mappingFildEntity : mappingFieldList){
                //字段名
                mapping.startObject(mappingFildEntity.getName());
                //字段类型
                if(null!=mappingFildEntity.getType()){
                    mapping.field("type",mappingFildEntity.getType().toString().toLowerCase());
                }

                //分析器
                if(StringUtils.isNotEmpty(mappingFildEntity.getAnalyzer())){
                    mapping.field("analyzer",mappingFildEntity.getAnalyzer());
                }
                //查询分析器
                if(StringUtils.isNotEmpty(mappingFildEntity.getSearchAnalyzer())){
                    mapping.field("search_analyzer",mappingFildEntity.getSearchAnalyzer());
                }
                if(ArrayUtils.isNotEmpty(mappingFildEntity.getFormat())){
                    mapping.field("format",mappingFildEntity.getFormat());
                }
                mapping.endObject();
            }
            mapping.endObject();
            mapping.endObject();
            mapping.flush();
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            mapping.close();
        }
        return mapping;
    }

    public XContentBuilder createMappingByEsColumnList(List<EsColumnEntity> esColumnList) {
        XContentBuilder mapping = null;
        if(CollectionUtils.isEmpty(esColumnList)){
            return mapping;
        }
        try{
            mapping = XContentFactory.jsonBuilder().startObject();
            mapping.startObject("properties");
            for(EsColumnEntity entity : esColumnList){
                //字段名
                mapping.startObject(entity.getColumn());
                //字段类型
                if(null!=entity.getType()){
                    mapping.field("type",entity.getEsType().toString().toLowerCase());
                }

               /* //分析器
                if(StringUtils.isNotEmpty(mappingFildEntity.getAnalyzer())){
                    mapping.field("analyzer",mappingFildEntity.getAnalyzer());
                }*/
               /* //查询分析器
                if(StringUtils.isNotEmpty(mappingFildEntity.getSearchAnalyzer())){
                    mapping.field("search_analyzer",mappingFildEntity.getSearchAnalyzer());
                }*/
                if(StringUtils.isNotEmpty(entity.getFormat())){
                    mapping.field("format",entity.getFormat());
                }
                mapping.endObject();
            }
            mapping.endObject();
            mapping.endObject();
            mapping.flush();
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            mapping.close();
        }
        return mapping;
    }
    /**********************************************ES服务器的一些操作方法**************************************/

}
