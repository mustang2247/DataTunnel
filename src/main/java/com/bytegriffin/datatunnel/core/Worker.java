package com.bytegriffin.datatunnel.core;


public class Worker implements Runnable {

    private HandlerContext next;
    private Param msg;
    private Handler handler;

    public Worker(Handler handler, HandlerContext next, Param msg) {
        this.handler = handler;
        this.next = next;
        this.msg = msg;
    }

    @Override
    public void run() {
        handler.channelRead(next, msg);
    }

}
