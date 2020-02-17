package com.econage.es.pool;

import com.econage.es.configure.ConfigureUtils;

import java.util.concurrent.Callable;

public abstract class EsTransportWithAutoConnection<V> implements Callable<V> {

    public EsTransportWithAutoConnection(){}
    @Override
    public V call() throws Exception {

        ClientService.getInstance().testServiceCofig(ConfigureUtils.configureEntity);
        return this.doAction();
    }

    protected abstract V doAction() throws Exception;
}
