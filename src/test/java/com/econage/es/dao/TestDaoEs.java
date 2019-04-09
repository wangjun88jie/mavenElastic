package com.econage.es.dao;

import com.econage.es.exception.ElasticException;
import com.econage.es.pool.ClientService;
import com.econage.es.search.AbstractEsSearchDao;
import com.econage.es.search.SearchBoolQueryBuilder;
import com.econage.es.search.SearchQueryBuilder;
import com.econage.es.search.dao.EsColumnEntity;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TestDaoEs extends AbstractEsSearchDao<TestEntity> {


    public List<TestEntity> queryList(TestSearchForm searchForm) throws Exception {
        List<SearchQueryBuilder> queryBuilders = new ArrayList<>();
        /*SearchQueryBuilder queryBuilder = getSearchQueryBuilder();
        queryBuilder.termsSearchForm(queryBuilder,"CREATE_USER","1511");
        queryBuilders.add(queryBuilder);*/
       SearchQueryBuilder queryBuilder2 = getSearchQueryBuilder();
        queryBuilder2.termsSearchForm(queryBuilder2,"CREATE_USER","0228");
        queryBuilders.add(queryBuilder2);


        SearchBoolQueryBuilder boolQueryBuilder = SearchBoolQueryBuilder.shouldBuilders(queryBuilders);
        return search(boolQueryBuilder,searchForm);
    }

    public void insertList(List<TestEntity> list) throws Exception {
        insertBulk(list);
    }

    public void insertSingleq(TestEntity entity) throws ElasticException {
        insertSingle(entity);
    }

    public static void main(String[] args) throws Exception {
        ClientService.getInstance().testServiceCofig();
        /*ClientService.getInstance().testServiceCofig();
        TestDaoEs testDao = new TestDaoEs();
        TestSearchForm searchForm = new TestSearchForm();
        //searchForm.setName("bill");
        searchForm.setPage(1);
        searchForm.setRows(5000);
        List<TestEntity> tt = testDao.queryList(searchForm);
        //Date date = DateUtils.parseDate("2017-2-2","yyyy-MM-dd");
        for(TestEntity testEntity : tt){
            System.out.println(testEntity.getName());
        }*/
       /* Pattern pattern = Pattern.compile(".*[A-Z].*");
        System.out.println(pattern.matcher("asDdf").matches());*/
        //throw new  ElasticException("qqq");
        List<TestEntity> list = new ArrayList<>();
        TestEntity e1 = TestEntity.create(1,"qqq1111111111","www",new Date());
        list.add(e1);
        TestEntity e2 = TestEntity.create(2,"qqq111111113343411","w5454ww",new Date());
        list.add(e2);
        TestDaoEs testDao = new TestDaoEs();
        testDao.insertBulk(list);
    }

    @Override
    protected boolean customCreate(String esIndex, String esType, List<EsColumnEntity> colsList) {
        return false;
    }
}
