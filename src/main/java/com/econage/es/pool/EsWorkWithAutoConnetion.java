package com.econage.es.pool;

import java.util.concurrent.Callable;

public abstract class EsWorkWithAutoConnetion<V> implements Callable<V> {

    public EsWorkWithAutoConnetion(){}
    @Override
    public V call() throws Exception {

        ClientService.getInstance().testServiceCofig();
        return this.doAction();
    }

    protected abstract V doAction() throws Exception;
}
