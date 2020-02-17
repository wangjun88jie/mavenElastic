package com.econage.es.search;

import com.econage.es.EsColumnProperty_;
import com.econage.es.EsId_;
import com.econage.es.EsIndex_;
import com.econage.es.configure.ConfigureUtils;
import com.econage.es.exception.ElasticException;
import com.econage.es.pool.ClientService;
import com.econage.es.pool.CommonVar;
import com.econage.es.search.dao.EsColumnEntity;
import com.econage.es.search.dao.OrderEntity;
import com.econage.es.search.searchEnum.EsColumnType;
import com.econage.es.search.service.EsDataService;
import com.econage.es.search.service.EsIndexService;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.sort.SortBuilders;

import java.io.IOException;
import java.lang.reflect.*;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public abstract class AbstractEsSearchDao<T> {
    //private static Logger logger = Logger.getLogger(AbstractSearchDao.class);
    private Class<T> entityClass;
    private Field[] fields;
    private List<EsColumnEntity> colsList;
    private Map<String,EsColumnEntity> fieldMap;
    private Map<String,EsColumnEntity> esColMap;
    protected SearchType searchType = SearchType.DFS_QUERY_THEN_FETCH;//搜索类型 DFS_QUERY_THEN_FETCH 搜索慢 准确高;
    private String esIndex;//ES索引
    private String esType;//ES类型
    private boolean isCreate;
    private int shards;
    private int replicas;
    private String esIdName;//主键字段名称
    private static Pattern pattern = Pattern.compile(".*[A-Z].*");
    public AbstractEsSearchDao(){
        try {
            Type type = getClass().getGenericSuperclass();
            Type trueType = ((ParameterizedType) type).getActualTypeArguments()[0];
            this.entityClass = (Class<T>) trueType;
            EsIndex_ esIndex_ = entityClass.getAnnotation(EsIndex_.class);
            if(esIndex_==null || StringUtils.isEmpty(esIndex_.es_index())){
                throw new ElasticException("【"+entityClass.getName()+"】未设置es_index注解");
            }
            this.esIndex = esIndex_.es_index();
            this.esType = esIndex_.es_type();
            this.isCreate = esIndex_.is_create();
            this.shards = esIndex_.shards_()==0?ConfigureUtils.configureEntity.getShards():esIndex_.shards_();
            this.replicas = esIndex_.replicas_()==0?ConfigureUtils.configureEntity.getReplicas():esIndex_.replicas_();
            this.fields = parseEntityField(entityClass);
            this.fieldMap = new HashMap<>();
            this.esColMap = new HashMap<>();
            this.colsList = colList();



            if(pattern.matcher(esIndex).matches()) {
                throw new ElasticException("【"+entityClass.getName()+"】的索引不能包含大写字母");
            }
            if(StringUtils.isNotEmpty(esIndex) && pattern.matcher(esType).matches()) {
                throw new ElasticException("【"+entityClass.getName()+"】的索引类型不能包含大写字母");
            }

            checkResource();
        }catch (ElasticException e){
            e.printStackTrace();
        }

    }

    /**
     * 1、检验索引库的相关信息
     * 2、如果校验不成功，是否自动创建
     */
    private void checkResource() throws ElasticException {
        if(isCreate && !EsIndexService.getInstance().indexExists(esIndex)){
            //创建索引以及mapping
            if(customCreate(esIndex,esType,colsList)){

            }else{
                //程序自动创建索引库
                EsIndexService.getInstance().createIndex(esIndex,shards,replicas);

                XContentBuilder mappingBuilder = EsIndexService.getInstance().createMappingByEsColumnList(colsList);
                if(mappingBuilder==null){
                    throw new ElasticException("【"+esIndex+"】创建mapping失败");
                }
                EsIndexService.getInstance().putMapping(esIndex,esType,mappingBuilder);
            }
        }else if(!isCreate&&!EsIndexService.getInstance().indexExists(esIndex)){
            //无权限创建index,且index服务器上不存在 则抛出异常
            throw new ElasticException("不存在索引库【"+esIndex+"】");
        }
    }



    /**
     * 自定义创建索引库
     * @param esIndex
     * @param esType
     * @param colsList
     * @return
     */
    protected  boolean customCreate(String esIndex, String esType, List<EsColumnEntity> colsList){
        return false;
    }

    protected List<T> search(BoolQueryBuilder queryBuilder,AbstractSearchForm form){
        //index 校验

        SearchRequestBuilder requestBuilder;
        if(Strings.isNullOrEmpty(esType)){
            requestBuilder = ClientService.getInstance().getClient().prepareSearch(esIndex).setSearchType(searchType);
        }else{
            requestBuilder = ClientService.getInstance().getClient().prepareSearch(esIndex).setTypes(esType).setSearchType(searchType);
        }
        requestBuilder.setQuery(queryBuilder);
        parserPageAndOrder(requestBuilder,form);//分页排序
        requestBuilder.setExplain(true);//设置是否按查询匹配度排序 查询优化
        SearchResponse response = requestBuilder.get();//执行查询
        List<T> list= fillEntity(response);
        return list;
    }

    //获取查询条件的工厂对象
    protected SearchQueryBuilder getSearchQueryBuilder(){
        return SearchQueryBuilder.createQueryBuilder(fieldMap,esColMap);
    }

    //解析返回的结果集
    private List<T> fillEntity(SearchResponse response) {
        Map<String, ProfileShardResult> profileMap = response.getProfileResults();
        SearchHits hits = response.getHits();
        TotalHits total = hits.getTotalHits();
        //logger.info(CommonVar.lOG_INFO+"total:"+total);
        SearchHit[] hitArray = hits.getHits();
        List<T> list = new ArrayList<>();
        if(ArrayUtils.isEmpty(hitArray)){
            return list;
        }
        for(SearchHit hit:hitArray){
            list.add(parseEntity(hit.getSourceAsMap()));
        }
        return list;
    }

    /**
     * 将map转成entity字段值
     * @param sourceAsMap
     * @return
     */
    private T parseEntity(Map<String,Object> sourceAsMap) {
       Object obj = getEntity();
        for(EsColumnEntity entity : colsList){
            parserColumnValue(entity,obj,sourceAsMap);
        }
        //get(obj,entityClass);
        return get(obj,entityClass);
    }

    /**
     * 根据字段属性，填充字段值
     * @param entity
     * @param obj
     */
    private void parserColumnValue(EsColumnEntity entity, Object obj,Map<String,Object> sourceAsMap) {
        Field field = null;
        try {
            field = obj.getClass().getDeclaredField(entity.getName());
            field.setAccessible(true);//设置对象的访问权限，保证对private的属性的访问
            if(sourceAsMap.get(entity.getColumn())!=null){
                //如果es的结果集有值,则进行实体对象填充
                if("int".equals(entity.getType()) && entity.getEsType().equals(EsColumnType.INTEGER)){
                    field.setInt(obj,Integer.parseInt(String.valueOf(sourceAsMap.get(entity.getColumn()))));
                }else if("long".equals(entity.getType()) && entity.getEsType().equals(EsColumnType.LONG)){
                    field.setLong(obj,Long.parseLong(String.valueOf(sourceAsMap.get(entity.getColumn()))));
                }else if("double".equals(entity.getType()) && entity.getEsType().equals(EsColumnType.DOUBLE)){
                    field.setDouble(obj,Double.parseDouble(String.valueOf(sourceAsMap.get(entity.getColumn()))));
                }else if("java.util.Date".equals(entity.getType()) && entity.getEsType().equals(EsColumnType.DATE)){
                    if(Strings.isNullOrEmpty(entity.getFormat())){
                        field.set(obj,DateUtils.parseDate(String.valueOf(sourceAsMap.get(entity.getColumn())),ConfigureUtils.configureEntity.getDefaultDateFormat()));
                    }else{
                        field.set(obj,DateUtils.parseDate(String.valueOf(sourceAsMap.get(entity.getColumn())),entity.getFormat()));
                    }
                }else{
                    field.set(obj,sourceAsMap.get(entity.getColumn()));
                }
            }


        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }catch (IllegalAccessException e) {
            e.printStackTrace();
        }catch (ParseException e){
            e.printStackTrace();
        }
    }


    /**
     * 根据字段属性，获取
     * @param entity
     * @param obj
     */
    private Object getValueByColumn(EsColumnEntity entity, Object obj) throws ElasticException {
        Field field = null;
        try {
            field = obj.getClass().getDeclaredField(entity.getName());
            field.setAccessible(true);//设置对象的访问权限，保证对private的属性的访问
            Object value = null;
            //如果es的结果集有值,则进行实体对象填充
            if("int".equals(entity.getType())){
                return field.getInt(obj);
            }else if("long".equals(entity.getType())){
                return field.getLong(obj);
            }else if("double".equals(entity.getType())){
                return field.getDouble(obj);
            }else if("java.util.Date".equals(entity.getType())){
                value = field.get(obj);
                if(value!=null){
                    return DateFormatUtils.format((Date)value,entity.getFormat());
                }
                return null;
            }else{
                return field.get(obj);
            }
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
            throw new ElasticException(e);
        }catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new ElasticException(e);
        }catch (Exception e){
            e.printStackTrace();
            throw new ElasticException(e);
        }
    }


    private Object getEntity(){
        Object obj = null;
        try {
            Constructor<?> cons = entityClass.getConstructor();
            obj = cons.newInstance(); // 为构造方法
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }finally {
            return obj;
        }

    }

    /**
     * 排序和分页
     * @param requestBuilder
     * @param form
     */
      private void parserPageAndOrder(SearchRequestBuilder requestBuilder, AbstractSearchForm form){
        if(requestBuilder==null){
            return ;
        }
        //排序
        if(CollectionUtils.isNotEmpty(form.getOrders())){
            List<OrderEntity> list = form.getOrders();
            for(OrderEntity entity : list){
                requestBuilder.addSort(SortBuilders.fieldSort(entity.getName()).unmappedType(entity.getType().toString()).order(entity.getDesc()));
            }
        }
        //分页
        if(form.getPage()>0 && form.getRows()>0){
            requestBuilder.setFrom((form.getPage()-1) * form.getRows()+1).setSize(form.getRows());
        }
    }

    //分析类中所有的域信息
    protected static Field[] parseEntityField(Class<?> cls) {
        Map<String,Field> fieldMap = new LinkedHashMap<String, Field>();
        do{
            for(Field field:cls.getDeclaredFields()){
                //Class<?> fieldType = field.getType();
                String fieldName   = field.getName();
                //域没有分析过，并且类型是支持的类型或者是枚举
                if(!fieldMap.containsKey(fieldName)){
                    fieldMap.put(fieldName,field);
                }
            }
            cls = cls.getSuperclass();
        }while(cls!=Object.class&&cls!=null);
        return fieldMap.values().toArray(new Field[]{});
    }

    /**
     * 获取实体类 字段的相关属性
     * @return
     */
    protected List<EsColumnEntity> colList(){
        if(fields==null || fields.length==0){
            return null;
        }
        List<EsColumnEntity> list = new ArrayList<EsColumnEntity>();
        EsColumnEntity entity;
        EsColumnProperty_ columnProperty_;
        EsId_ esId_;
        for(Field field:fields){
            entity = new EsColumnEntity();
            //处理seId 主键字段
            esId_ = field.getAnnotation(EsId_.class);
            columnProperty_ = field.getAnnotation(EsColumnProperty_.class);
            if(esId_!=null){
                esIdName = field.getName();
            }
            if(columnProperty_!=null){
                //有字段注解
                entity.setName(field.getName());
                if(!Strings.isNullOrEmpty(columnProperty_.column())){
                    entity.setColumn(columnProperty_.column());
                }
                if(!Strings.isNullOrEmpty(columnProperty_.format())){
                    entity.setFormat(columnProperty_.format());
                }

                if(columnProperty_.type()!=null){
                    entity.setEsType(columnProperty_.type());
                }
            }else{
                //todo 没有字段注解 一些缺省项的处理
                entity.setColumn(EsUtils.camelToUnderline(field.getName()));
                if("int".equals(entity.getType())){
                   entity.setEsType(EsColumnType.INTEGER);
                }else if("long".equals(entity.getType()) && entity.getEsType().equals(EsColumnType.LONG)){
                    entity.setEsType(EsColumnType.LONG);
                }else if("double".equals(entity.getType())){
                    entity.setEsType(EsColumnType.DOUBLE);
                }else if("java.util.Date".equals(entity.getType())){
                    entity.setEsType(EsColumnType.DOUBLE);
                }
            }
            entity.setType(field.getType().getCanonicalName());
            fieldMap.put(entity.getName(),entity);
            esColMap.put(entity.getColumn(),entity);
            list.add(entity);
        }
        return list;
    }

    private static <T> T get(Object o,Class<T> clazz){
        if(clazz!=null){
            if(clazz.isInstance(o))
                return clazz.cast(o);
            else
                throw new RuntimeException(o +" is not a "+clazz.getName());
        }
        return null;
    }


    protected void insertBulk(List<T> list) throws ElasticException {
        insertAndUpdateBulk(list,"index");
    }

    protected void deleteById(String id) throws ElasticException {
        EsDataService.getInstance().deleteById(esIndex,esType,id);
    }

    protected void updateAllBulk(List<T> list) throws ElasticException {
        insertAndUpdateBulk(list,"update");
    }

    protected void updateBulkByColumn(List<T> list,String[] columnArr) throws ElasticException {
       //todo 部分字段更新
    }

    protected void upsertBulk(List<T> list) throws ElasticException {
        insertAndUpdateBulk(list,"upsert");
    }
    //批量新增
    private void insertAndUpdateBulk(List<T> list,String updateType) throws ElasticException {
        TransportClient client = ClientService.getInstance().getClient();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        XContentBuilder xContentBuilder;
        try {
            String esId;
            for(T entity : list){
                esId = String.valueOf(getValueByColumn(fieldMap.get(esIdName),entity));
                xContentBuilder = jsonBuilder();
                parseXContentBuilder(xContentBuilder,entity);
                if(updateType.equalsIgnoreCase("index")){
                    bulkRequest.add(client.prepareIndex(esIndex, esType, esId)
                            .setSource(xContentBuilder));
                }else if(updateType.equalsIgnoreCase("upsert")){
                    bulkRequest.add(client.prepareUpdate(esIndex,
                            esType, esId)
                            .setDoc(xContentBuilder).setDocAsUpsert(true));
                } else if (updateType.equalsIgnoreCase("update")) {
                    bulkRequest.add(client.prepareUpdate(esIndex, esType, esId)
                            .setDoc(xContentBuilder));
                } else {
                    throw new ElasticException("Index operation: 【" + updateType + "】 not supported.");
                }

            }
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if (bulkResponse.hasFailures()) {
                //服务器返回错误
                throw new ElasticException(bulkResponse.buildFailureMessage());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(client!=null){
                ClientService.getInstance().returnObject(client);
            }
        }
    }


    protected void insertSingle(T entity) throws ElasticException {
        insertSingle(entity,"index");
    }

    protected void updateAllSingle(T entity) throws ElasticException {
        insertSingle(entity,"update");
    }

    protected void upsertSingle(T entity) throws ElasticException {
        insertSingle(entity,"upsert");
    }

    protected void updateSingleByColumn(T entity,String[] columnArr) throws ElasticException {
        //todo 部分字段更新

    }


    private void insertSingle(T entity,String updateType) throws ElasticException {
        TransportClient client = null;
        try {
            String esId = String.valueOf(getValueByColumn(fieldMap.get(esIdName),entity));
            XContentBuilder xContentBuilder = jsonBuilder();
            parseXContentBuilder(xContentBuilder,entity);
            ClientService.getInstance().getClient();
            if (updateType.equalsIgnoreCase("index")) {
                client.prepareIndex(esIndex, esType, esId).
                        setSource(xContentBuilder).get();
            } else if (updateType.equalsIgnoreCase("upsert")) {
                client.prepareUpdate(esIndex, esType, esId).
                        setDoc(xContentBuilder).setDocAsUpsert(true).get();
            } else if (updateType.equalsIgnoreCase("update")) {
                client.prepareUpdate(esIndex, esType, esId).
                        setDoc(xContentBuilder).get();
            }
        }catch (Exception e){
            e.printStackTrace();
            throw  new  ElasticException("",e);
        }finally {
            if(client!=null){
                ClientService.getInstance().returnObject(client);
            }
        }
    }





    private void parseXContentBuilder(XContentBuilder xContentBuilder, T entity) throws IOException, ElasticException {
        if(CollectionUtils.isEmpty(colsList)){
            throw new ElasticException("未获取字段信息");
        }
        xContentBuilder.startObject();
        for(EsColumnEntity columnEntity : colsList){
            xContentBuilder.field(columnEntity.getColumn(),getValueByColumn(columnEntity,entity));
        }
        xContentBuilder.endObject();
    }


}
