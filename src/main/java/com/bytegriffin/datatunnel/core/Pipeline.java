package com.bytegriffin.datatunnel.core;

public class Pipeline {

    private HandlerContext head;

    public void addFirst(Handler handler) {
        HandlerContext ctx = new HandlerContext(handler);
        HandlerContext tmp = head;
        head = ctx;
        head.setNext(tmp);
    }

    public Pipeline() {
        head = new HandlerContext(null);
    }

    public void request(Param msg) {
        head.doWork(msg);
    }

}