package com.econage.es;

import static org.junit.Assert.assertTrue;

import com.econage.es.exception.ElasticException;
import com.econage.es.pool.ClientService;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() throws ElasticException {
        ClientService.getInstance().testServiceCofig();
        //boolean is = EsUtils.indexExists("testindex");
      /*  Settings settings = EsUtils.createSettingsBuilder(5,0).build();
        List<MappingFildEntity> list = new ArrayList<MappingFildEntity>();
        MappingFildEntity entity = new MappingFildEntity();
        entity.setName("ID_");
        entity.setType(EsColumnType.INTEGER);
        list.add(entity);
        MappingFildEntity entity1 = new MappingFildEntity();
        entity1.setName("NAME_");
        entity1.setType(EsColumnType.TEXT);
        list.add(entity1);
        XContentBuilder mapping = EsUtils.createMapping("test_type_",list);

        EsUtils.createIndex("testindex",settings,mapping);*/
        //todo 删除索引
        //EsUtils.deleteIndex("testindex");
        //todo 创建索引
       //EsUtils.createIndex("testindex", EsUtils.createSettingsBuilder(5,0).build());


        //System.out.println(is);
        //assertTrue( true );

        //todo 设置mapping信息
       /* List<MappingFildEntity> list = new ArrayList<MappingFildEntity>();
        MappingFildEntity entity = new MappingFildEntity();
        entity.setName("ID_");
        entity.setType(EsColumnType.INTEGER);
        list.add(entity);
        MappingFildEntity entity1 = new MappingFildEntity();
        entity1.setName("NAME_");
        entity1.setType(EsColumnType.TEXT);
        list.add(entity1);

        EsUtils.putMapping("testindex","test_type_",EsUtils.createMapping(list));*/
        //todo 新增记录
       List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        Map<String,Object> map1 = new HashMap<>();
        map1.put("ES_ID","1");
        map1.put("ID_","1");
        map1.put("NAME_","wkk");
        list.add(map1);
        Map<String,Object> map2 = new HashMap<>();
        map2.put("ES_ID","2");
        map2.put("ID_","2");
        map2.put("NAME_","rrrr");
        list.add(map2);
        //EsUtils.createDocBulk(list,"testindex","test_type_","index");


    }
}
