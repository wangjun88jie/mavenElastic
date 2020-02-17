package com.econage.es.exception;

public class ElasticInitException extends Exception{
    private static String sign = "######ES#####init:";//es初始化异常标识
    public ElasticInitException(){
        super();
    }

    public ElasticInitException(String message){
        super(sign+message);
    }

    public ElasticInitException(String message, Throwable cause){
        super(sign+message);
    }

    public ElasticInitException(Throwable cause) {
        super(cause);
    }
}
