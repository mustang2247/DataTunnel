package com.bytegriffin.datatunnel.core;

public interface Handler {

    void channelRead(HandlerContext ctx, Param msg);

}