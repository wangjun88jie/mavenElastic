package com.econage.es.search;


import com.econage.es.exception.ElasticException;
import com.econage.es.pool.ClientService;
import org.apache.poi.ss.formula.functions.T;
import org.elasticsearch.client.transport.TransportClient;

public abstract class AbstractEsClient {
    protected T runTransaction(TransactionAction action) throws ElasticException {
        ClientService service = ClientService.getInstance();
        TransportClient client = service.getClient();
        try{
            return action.execute(client);
        }catch(Exception s){
            s.printStackTrace();
            throw new ElasticException(s);
        }finally {
            service.returnObject(client);

        }
    }


    protected boolean runTransactionBoolean(TransactionActionBoolean action) throws ElasticException {
        ClientService service = ClientService.getInstance();
        TransportClient client = service.getClient();
        try{
            return action.executeBoolean(client);
        }catch(Exception s){
           throw new ElasticException(s);
        }finally {
            service.returnObject(client);

        }
    }

    protected abstract class TransactionAction{
        protected abstract T execute(TransportClient client) throws ElasticException;
        protected boolean ignoreException(){
            return false;
        }
    }

    protected abstract class TransactionActionBoolean{
        protected abstract boolean executeBoolean(TransportClient client) throws ElasticException;
        protected boolean ignoreException(){
            return false;
        }
    }

}
