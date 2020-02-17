package com.econage.es.pool;

import com.econage.es.configure.ConfigureUtils;

import java.util.concurrent.Callable;

public abstract class EsRestWithAutoConnection<V> implements Callable<V> {

    public EsRestWithAutoConnection(){}
    @Override
    public V call() throws Exception {

        RestHighClientService.getInstance().testServiceCofig(ConfigureUtils.configureEntity);
        return this.doAction();
    }

    protected abstract V doAction() throws Exception;
}
