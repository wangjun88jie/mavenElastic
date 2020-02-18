package com.econage.es;

import com.econage.es.pool.RestHighClientService;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * es操作的一些demo
 */
public class Test {


    /**
     * ping集群
     * @throws IOException
     */
     public void  pingTest() throws IOException {
         boolean response =  RestHighClientService.getInstance().getClient().ping(RequestOptions.DEFAULT);//用来判断连接是否可用
         System.out.println();
     }

    /**
     * 索引是否存在
     * @throws IOException
     */
    public void existsIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("index_test1");
        boolean response =  RestHighClientService.getInstance().getClient().indices().exists(request,RequestOptions.DEFAULT);
        System.out.println();
    }


    /**
     * 创建索引
     * @throws IOException
     */
    public void createIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("index_test");//设置索引名称(不能含大写字母)
        request.settings(Settings.builder().put("index.number_of_shards",1)
                .put("index.number_of_replicas",0));//设置分片
        //创建mapper
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
            builder.startObject("properties");
                builder.startObject("message");
                    builder.field("type", "text");
                builder.endObject();
            builder.endObject();
        builder.endObject();
        request.mapping(builder);
        //创建索引
        CreateIndexResponse createIndexResponse = RestHighClientService.getInstance().getClient().indices().create(request, RequestOptions.DEFAULT);
        System.out.println("索引创建成功！");
    }

    /**
     * 删除索引
     * @throws IOException
     */
    public void deleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("index_test");//索引名
        AcknowledgedResponse acknowledgedResponse = RestHighClientService.getInstance().getClient().indices().delete(request, RequestOptions.DEFAULT);
        System.out.println("索引删除成功！");
    }


    //todo mapper更新

    //todo Setting更新






    /**
     * 前提:索引分片index置为只读read-only去掉   "read_only_allow_delete": "false"
     * 插入数据(单条)
     * @throws IOException
     */
    public void insertSingleData() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        //document数据包装
        builder.startObject();
            builder.field("message", "测试");
        builder.endObject();
        IndexRequest indexRequest = new IndexRequest("index_test")
                .id("1").source(builder);
        RestHighClientService.getInstance().getClient().index(indexRequest,RequestOptions.DEFAULT);
    }

    /**
     * 前提:索引分片index置为只读read-only去掉   "read_only_allow_delete": "false"
     * 批量插入
     * @throws IOException
     */
    public void insertBulkData() throws IOException {
      /*
      该写法待测
      BulkRequest request = new BulkRequest();
        XContentBuilder builder1 = XContentFactory.jsonBuilder();
        builder1.startObject();
        builder1.field("message", "测试2");
        builder1.endObject();

        XContentBuilder builder2 = XContentFactory.jsonBuilder();
        builder1.startObject();
        builder1.field("message", "测试3");
        builder1.endObject();

        request.add(new IndexRequest("index_test").id("2")
                .source(builder1));
        request.add(new IndexRequest("index_test").id("3")
                .source(builder2));
        RestHighClientService.getInstance().getClient().bulk(request,RequestOptions.DEFAULT);*/
        BulkRequest request = new BulkRequest();

        request.add(new IndexRequest("index_test").id("2")
                .source(XContentType.JSON,"message", "测试2"));
        request.add(new IndexRequest("index_test").id("3")
                .source(XContentType.JSON,"message", "测试3"));

        RestHighClientService.getInstance().getClient().bulk(request,RequestOptions.DEFAULT);
    }


    /**
     * 查询单条数据
     * @throws IOException
     */
    public void getSingle() throws IOException {
        RestHighLevelClient client = RestHighClientService.getInstance().getClient();
        GetRequest getRequest = new GetRequest(
                "index_test",//索引
                "_doc",//类型
                "1");
        GetResponse getResponse = client.get(getRequest,RequestOptions.DEFAULT);
        System.out.println();
    }


    /**
     * 前提:索引分片index置为只读read-only去掉   "read_only_allow_delete": "false"
     * 删除数据
     * @throws IOException
     */
    public void delete() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest ("index_test","1");
        RestHighClientService.getInstance().getClient().delete(deleteRequest,RequestOptions.DEFAULT);
    }


    public void searchMatchAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //QueryBuilders 可以构建各种search query是es查询核心类
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.indices("index_test");//指定索引
        searchRequest.source(searchSourceBuilder);
        RestHighLevelClient client = RestHighClientService.getInstance().getClient();
        SearchResponse searchResponse = client.search(searchRequest,RequestOptions.DEFAULT);
        SearchHits searchHits = searchResponse.getHits();//数据结果集
        System.out.println();
    }












}
