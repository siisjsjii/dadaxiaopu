package com.cinfly.dadaxiaopu.utils;

public interface ILock {
    boolean tryLock(Long timeoutSec);
    void unLock();
}
