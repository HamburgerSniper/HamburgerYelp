package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * 基于redis的id全局唯一id生成器
 */
@Component
public class RedisIdWorker {
    // 记录开始时间戳
    private static final long BEGIN_TIMESTAMP = 1675209600L;
    // 序列号的位数
    private static final long COUNT_BITS = 32;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    // keyPrefix用于区分不同业务的前缀
    public long nextId(String keyPrefix) {
        // 1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN_TIMESTAMP;
        // 2.生成序列号 --> 利用redis自增长
        // 不能简单写成 "icr"+keyPrefix+":" 这样所有的业务用的都是同一个key，上限为2的64次幂
        // 故在此我们在后面再拼接上一个日期时间戳，每天下的单用一个自增key，以后也方便进行统计
        // 2.1获取当前日期，精确到天，如果想要统计以月为单位的总下单量，则ofPattern中的参数可以写成
        // yyyy:MM:dd的形式，Redis中遇到冒号会自动将key分隔开，因此统计月总下单量只需要统计以
        // yyyy:MM开头的数据量即可
        String date = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        // 2.2自增长
        Long count = stringRedisTemplate.opsForValue().increment("icr" + keyPrefix + ":" + date);
        // 3.拼接并返回 --> 返回值要求为long，因此不能采用简单的字符串拼接(返回值类型为String)，采用位运算左移
        // 末尾拼接采用或运算 |
        return timestamp << COUNT_BITS | count;
    }

    public static void main(String[] args) {
        LocalDateTime time = LocalDateTime.of(2023, 2, 1, 0, 0, 0);
        long second = time.toEpochSecond(ZoneOffset.UTC);
        System.out.println("second" + second);
    }
}
