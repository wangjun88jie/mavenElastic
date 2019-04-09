package com.econage.es;

import com.econage.es.pool.EsWorkWithAutoConnetion;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception {
        new EsWorkWithAutoConnetion(){

            @Override
            protected Object doAction() throws Exception {



                return null;
            }
        }.doAction();

    }
}
