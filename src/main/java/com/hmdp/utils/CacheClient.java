package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class CacheClient {
    /**
     * TODO 工具类中的方法不用static修饰的原因，是因为方法中使用的stringRedisTemplate是通过ioc注入进来的
     * TODO ioc通过new的方式创建Bean，new出来的对象实在堆里面，static修饰的对象优先于对象存在
     */
    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        // 将Object类型的value对象序列化为String类型的JSON字符串
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        // (逻辑过期的本质其实是永久有效)
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type,
                                          Function<ID, R> dbFallback,
                                          Long time, TimeUnit unit) {
        // 拼出来得到要写入缓存中的key
        String key = keyPrefix + id;
        // 1.从redis查询对应的__缓存，得到的是JSON数据
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (StringUtils.isNotBlank(json)) {
            // 3.存在，则直接返回 --> 使用JSONUtil工具类，把shopJson数据转换成Shop类的数据
            return JSONUtil.toBean(json, type);
        }
        // 判断命中的是否为空值
        if (json != null) {
            return null;
        }

        // 4.不存在，则根据id查询数据库 --> 最终返回的也需要是R，但是数据库不同，我们根本不可能知道是什么
        // 因此交给调用方 --> 函数式编程 --> 有参有返回值的函数 Function
        R r = dbFallback.apply(id);
        // 5.不存在，返回错误
        if (r == null) {
            // 将空值写入redis ，这里有个小问题，空值的过期时间和正常值的过期时间是一样的
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, unit);
            // 返回错误信息
            return null;
        }
        // 6.存在，写入redis，写入的也是JSON数据，因此需要JsonStr工具类
        this.set(key, r, time, unit);
        // 7.返回
        return r;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type,
                                            Function<ID, R> dbFallback,
                                            Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从redis查询缓存，得到的是JSON数据
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (StringUtils.isBlank(json)) {
            // 3.不存在，直接返回null
            return null;
        }
        // 4.命中，需要先把JSON数据反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5.1 未过期则直接返回
            return r;
        }
        // 5.2 已经过期，需要进行缓存重建 —— 互斥锁
        // 6.缓存重建
        // 6.1 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 6.2 判断是否获取锁成功
        if (isLock) {
            // 6.3 TODO 成功，开启独立线程，实现缓存重建
            // 尽量使用线程池操作，性能更好
            // 注意：获取锁成功应该再次检测redis缓存是否过期，做DoubleCheck
            // 如果存在则无需重建缓存
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 重建缓存
                    // 查询数据库
                    R r1 = dbFallback.apply(id);
                    // 写入redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放锁 --> 非常重要！！！
                    unlock(lockKey);
                }
            });
        }
        // 6.4 返回过期的信息
        return r;
    }

    /**
     * 尝试获取锁的方法
     *
     * @param key
     * @return
     */
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁
     *
     * @param key
     */
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

}
