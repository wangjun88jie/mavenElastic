package com.econage.es;

import com.econage.es.pool.RestHighClientService;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

/**
 * es操作的一些demo
 */
public class Test {




     public void  pingTest() throws IOException {
         boolean response =  RestHighClientService.getInstance().getClient().ping(RequestOptions.DEFAULT);//用来判断连接是否可用
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


    /**
     * 插入数据
     * @throws IOException
     */
    public void insertData(){

    }



    public void insertData2() throws IOException {
        RestHighLevelClient client = RestHighClientService.getInstance().getClient();
        GetRequest getRequest = new GetRequest(
                "file_data",//索引
                "_doc",//类型
                "11654");


        GetResponse getResponse = client.get(getRequest,RequestOptions.DEFAULT);
        System.out.println();
    }




}
