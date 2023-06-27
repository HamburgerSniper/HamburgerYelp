package com.hmdp.utils;

public interface ILock {

    /**
     * 尝试获取锁 (只获取一次 成功的话就成功 失败的就是失败 不会一直等在那么)
     * @param timeoutSec  设置超时的时间 怕你突然死机
     * @return
     */
    boolean tryLock(long timeoutSec);

    /**
     * 释放锁
     */
    void unLock();
}