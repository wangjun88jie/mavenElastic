package com.econage.es.search;
import com.econage.es.exception.ElasticException;
import org.apache.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;


/**
 * 一些ES服务器工具方法
 */
public class EsUtils{
    private static Logger logger = Logger.getLogger(EsUtils.class);
/**********************************************一些工具***************************************************/
    /**
     * 数据的空值判断
     * @param key
     * @param map
     * @return
     */
    public static Object fillValue(String key,EsDataMap map){
        if(map.get(key)!=null){
            return map.get(key);
        }
        //todo 空值定义 待定
        return "";
    }



    /**
     * 驼峰格式字符串转换为下划线格式字符串
     *
     * @param param
     * @return
     */
    private static final char UNDERLINE = '_';
    public static String camelToUnderline(String param) {
        if (param == null || "".equals(param.trim())) {
            return "";
        }
        int len = param.length();
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            char c = param.charAt(i);
            if (Character.isUpperCase(c)) {
                sb.append(UNDERLINE);
                sb.append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }


    /**
     * todo 待优化
     * @param client
     * @param index
     * @param type
     * @param edId
     * @param xContentBuilder
     * @param updateType
     * @throws ElasticException
     */
    public static void setPrepare(TransportClient client,String index,String type,String edId,
                                  XContentBuilder xContentBuilder,String updateType) throws ElasticException {
        if (updateType.equalsIgnoreCase("index")){
            client.prepareIndex(index, type, edId).
                    setSource(xContentBuilder).get();
        } else if (updateType.equalsIgnoreCase("upsert")){
            client.prepareUpdate(index, type, edId).
                    setDoc(xContentBuilder).setDocAsUpsert(true).get();
        } else if (updateType.equalsIgnoreCase("update")){
            client.prepareUpdate(index, type, edId).
                    setDoc(xContentBuilder).get();
        }else{
            throw new ElasticException("Index operation: 【" + updateType + "】 not supported.");
        }
    }
}
