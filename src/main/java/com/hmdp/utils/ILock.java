package com.hmdp.utils;

/**
 * @author 林太翔
 * @date 2025/1/26 13:31
 */
public interface ILock {
    // 获取锁
    boolean tryLock(long timeoutSec);
    // 释放锁
    void unlock();
}
