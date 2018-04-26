package com.bytegriffin.datatunnel.core;

public class HandlerContext {

    private Handler handler;
    private HandlerContext next;

    public HandlerContext(Handler handler) {
        this.handler = handler;
    }

    public void setNext(HandlerContext ctx) {
        this.next = ctx;
    }

    public void doWork(Param msg) {
        if (next == null) {
            return;
        }
//    	executor = Executors.newFixedThreadPool(3);
//    	executor.submit(new Worker(handler, next, msg));
        handler.channelRead(next, msg);
    }

    public void write(Param msg) {
        doWork(msg);
    }

}