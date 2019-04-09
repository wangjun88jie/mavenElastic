package com.econage.es.exception;

public class ElasticException extends Exception{
    private static String sign = "######ES#####:";//es异常标识
    public ElasticException(){
        super();
    }

    public ElasticException(String message){
        super(sign+message);
    }

    public ElasticException(String message,Throwable cause){
        super(sign+message);
    }

    public ElasticException(Throwable cause) {
        super(cause);
    }
}
