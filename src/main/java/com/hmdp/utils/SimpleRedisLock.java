package com.hmdp.utils;

import cn.hutool.core.lang.ResourceClassLoader;
import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SimpleRedisLock implements ILock {

    private StringRedisTemplate stringRedisTemplate;

    private String name;

    // 构造函数
    public SimpleRedisLock(StringRedisTemplate stringRedisTemplate, String name) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.name = name;
    }

    private static final String KEY_PREFIX = "lock";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";

    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        // 这个classpathresouce就是直接在我们的resource里面去找
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unLock.lua"));
        // 返回值
        UNLOCK_SCRIPT.setResultType(Long.class);

    }

    @Override
    public boolean tryLock(long timeoutSec) {
        // 重新弄一个 因为你要确保唯一性 然后同一线程 的肯定相同

        // 获取当前线程的表示
        String threadId = ID_PREFIX + Thread.currentThread().getId();

        // 获取锁 setIfAbsent(代表有死机的情况)
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + name, threadId, timeoutSec, TimeUnit.SECONDS);

        // 注意直接返回会有拆箱的操作(有线程不安全的风险)  所以直接返回基本类型
        // 避免空指针的风险
        return Boolean.TRUE.equals(flag);
    }

    @Override
    public void unLock() {
        // 调用lua脚本
        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX + name),
                ID_PREFIX + Thread.currentThread().getId()
        );


    }

    /*@Override
    public void unLock() {
        // 获取线程标识
        String threadId = ID_PREFIX + Thread.currentThread().getId();

        // 获取锁中的标识
        String id = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);

        if (id.equals(threadId)) {
            // 释放锁
            stringRedisTemplate.delete(KEY_PREFIX + name);
        }

    }*/
}