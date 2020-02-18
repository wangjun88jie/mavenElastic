package com.econage.es;

import com.econage.es.configure.ConfigureUtils;
import com.econage.es.pool.ClientService;
import com.econage.es.pool.EsRestWithAutoConnection;
import com.econage.es.pool.RestHighClientService;
import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

public class Test {



    public static void main1(String[] ss) throws UnknownHostException {


        /*logger.info(CommonVar.lOG_INFO+clusterName);InternalTDigestPercentiles
        logger.info(CommonVar.lOG_INFO+host);
        logger.info(CommonVar.lOG_INFO+port);*/
        // 192.168.0.70
            /*
            TransportClient client = new PreBuiltTransportClient(settings)
                    .addTransportAddresses(new TransportAddress(InetAddress.getByName("10.66.98.11"), 9300));*/
        //List<TransportAddress> aa =  client.transportAddresses();
        //client.prepareGet().;
        //client.filteredNodes();
        /*TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("host1"), 9300))
                .addTransportAddress(new TransportAddress(InetAddress.getByName("host2"), 9300));*/
        Settings settings = Settings.builder().put("cluster.name", "sc-es01").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("10.66.98.10"), 9300))
                .addTransportAddress(new TransportAddress(InetAddress.getByName("10.66.98.11"), 9300))
                .addTransportAddress(new TransportAddress(InetAddress.getByName("10.66.98.12"), 9300));
        /*Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.0.70"), 9200))
               */ ;

        //GetRequestBuilder builder = client.prepareGet("filedata","_doc","471104");

        SearchRequestBuilder requestBuilder = client.prepareSearch("filedata","386706");
        SearchResponse response = requestBuilder.get();

        SearchHits ddd = response.getHits();
        System.out.println(111);
        }

    public static void main(String[] args) throws Exception {

        new EsRestWithAutoConnection(){
            @Override
            protected Object doAction() throws Exception {
                /*RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("192.168.0.112",9200,"http")));
       */
                RestHighClientService.getInstance().testServiceCofig(ConfigureUtils.configureEntity);
                RestHighLevelClient client = RestHighClientService.getInstance().getClient();
                GetRequest getRequest = new GetRequest(
                        "file_data",//索引
                        "_doc",//类型
                        "11654");
                GetResponse getResponse = client.get(getRequest,RequestOptions.DEFAULT);
                System.out.println();
                return null;
            }
        }.doAction();
    }

}
