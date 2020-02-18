package com.econage.es;

import com.econage.es.configure.ConfigureUtils;
import com.econage.es.pool.EsTransportWithAutoConnection;
import com.econage.es.pool.RestHighClientService;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception {
        new EsTransportWithAutoConnection(){
            @Override
            protected Object doAction() throws Exception {
                RestHighClientService.getInstance().testServiceCofig(ConfigureUtils.configureEntity);
                Test test = new Test();
                test.searchMatchAll();
                //test.createIndex();
                //test.deleteIndex();
                return null;
            }
        }.doAction();

    }
}
