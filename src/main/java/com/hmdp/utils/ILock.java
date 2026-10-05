package com.hmdp.utils;

public interface ILock {
    boolean tryLock(Long timeoutSec);
    void unLock();
}
