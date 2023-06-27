package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {
    /**
     * TODO 工具类中的方法不用static修饰的原因，是因为方法中使用的stringRedisTemplate是通过ioc注入进来的
     * TODO ioc通过new的方式创建Bean，new出来的对象实在堆里面，static修饰的对象优先于对象存在
     */
    // 注入我们的redis
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 这个就是防止缓存穿透(这个就是防止穿透的)
     *
     * @param keyPrefix  是key的前缀
     * @param id         是去查数据库的(id类型也不能确定)
     * @param type       表示去redis里面查到的数据然后转为java对象
     * @param dbFallback 这个表示我们的查询数据库的那个方法
     * @param time       表示我们商品在redis里面存放的时间
     * @param unit       表示单位
     * @param <R>        这个就是我们的返回值
     * @param <ID>       这个就是我们id的类型
     * @return
     */
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;

        // 取redis中的缓存
        String json = stringRedisTemplate.opsForValue().get(key);

        // 进了这个判断就是有值的情况了
        if (StrUtil.isNotBlank(json)) {  //isNotBlank 不为空就是true
            R r = JSONUtil.toBean(json, type);
            return r;

        }

        // 如果进了这个判断就是空字符串
        if (json != null) {
            // 返回一个错误信息
            // 这个就表示你要查询的是错误的就直接返回空字符串(防止缓存穿透)
            return null;
        }

        // 查询数据库(这个地方返回的也是R 但是我们不知道数据库是谁 所以我们要写函数式接口)
        R r = dbFallback.apply(id);
        if (r == null) {
            // 将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 返回错误信息
            return null;
        }

        // 存入redis里面去
//        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(r), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        this.set(key, r, time, unit);
        return r;
    }


    // TODO 防止缓存击穿的线程池
    // TODO 成功 就开启一个线程(实现缓存重建)
    // 使用线程池来开启线程(10个线程)
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    /**
     * 这也是防止缓存击穿(设置的逻辑过期)
     *
     * @param keyPrefix  表示前缀
     * @param id         表示我们要查询数据库的id
     * @param type       表示我们从redis中的得到的数据转为什么java对象
     * @param dbFallback 函数式方程
     * @param time       设置过期的时间
     * @param unit       设置过期的单位
     * @param <R>        这个就是我们的返回值
     * @param <ID>       这个就是我们id的类型
     * @return
     */
    public <R, ID> R queryWithLogiclExpire(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {

        String key = keyPrefix + id;

        // 取redis中的缓存
        String jsonStr = stringRedisTemplate.opsForValue().get(key);

        // 进了这个判断就是没有命中
        if (StrUtil.isBlank(jsonStr)) {  //isNotBlank 不为空就是true
            return null;
        }

        // 命中需要json反序列化
        RedisData redisData = JSONUtil.toBean(jsonStr, RedisData.class);
        // 获取我们的数据
        // 得到我们的jsonObject
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);

        // 获取过期时间
        LocalDateTime expireTime = redisData.getExpireTime();

        // 再来判断是否过期(过期时间是不是在当前时间之后喃)
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 没有过期直接返回
            return r;
        }
        // 如果已经过期 我们就要进行缓存重建
        String lockKey = LOCK_SHOP_KEY + id;
        // 获取互斥锁
        boolean isLock = tryLock(lockKey);
        // 判断获取锁是否成功
        if (isLock) {
            // TODO 成功 就开启一个线程(实现缓存重建)
            // 提交任务
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // TODO 重建缓存
                    // 第一步查询数据库
                    R r1 = dbFallback.apply(id);

                    // 写入redis(并且使用的是逻辑过期)
                    this.setWithLogiclExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 重建缓存后就要释放锁
                    unlock(lockKey);
                }

            });
        }
        // 不成功就返回当前的数据(也就是旧数据)

        // 返回
        return r;
    }


    /**
     * 这个是获取锁
     *
     * @param key
     * @return
     */
    private boolean tryLock(String key) {
        // 相当于setnx 就是如果有的话你就设置不成功  LOCK_SHOP_TTL 10秒
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);

        // 不能直接返回 因为有拆箱的时候可能会发生风险
        return BooleanUtil.isTrue(flag);

    }

    /**
     * 释放锁
     *
     * @param key
     */
    private void unlock(String key) {
        // 将我们的setnx的那个键给删除 就相当于其他的线程就可以获取我们的数据了
        stringRedisTemplate.delete(key);
    }


    /**
     * 存入redis并设置过期时间(互斥锁)(设置空字符串也行)
     *
     * @param key   表示我们存入redis里面的key
     * @param value 表示我们存入redis里面的value
     * @param time  表示我们存入redis的时间
     * @param unit  表示我们的单位
     */
    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }


    /**
     * 这个也是解决我们的缓存击穿的问题(设计逻辑过期)(注意这个前提是热点商品然后key同时实现)
     * 这个不能保证缓存穿透的问题
     *
     * @param key   表示我们存入redis里面的key
     * @param value 表示我们存入redis里面的value
     * @param time  表示我们存入redis的时间
     * @param unit  表示我们的单位
     */
    public void setWithLogiclExpire(String key, Object value, Long time, TimeUnit unit) {
        // 我们定义了一个类 redisdata里面就有过期的字段
        RedisData redisData = new RedisData();
        redisData.setData(value);
        // 在当前的基础上再加上多少秒 我们这里就是统一转为秒来处理的  unit.toSeconds(time)
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));

        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }
}
