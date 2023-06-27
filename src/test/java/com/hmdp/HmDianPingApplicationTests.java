package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {

    @Autowired
    private RedisIdWorker redisIdWorker;

    // 生成线程  500个线程
    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IShopService shopService;

    @Test
    void loadShopData() {
        // 1.查询店铺信息(查询的是所有)
        List<Shop> list = shopService.list();
        // 2.把店铺分组 按照typeId来分组 id一致的放在一个集合
        // key就是我们的typeId(我们可以使用循环的方式来实现 但是我们现在可以使用steam流来实现)
        // 这个就会自动的实现
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));

        // 3.分批完成写入redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {

//          3.1.  key是类型id
            Long typeId = entry.getKey();

            // 这个就是存入redis的那个key
            String key = SHOP_GEO_KEY + typeId;


//            3.2. 在获取同类型的店铺集合
            List<Shop> value = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(value.size());

            // 3.3 写入redis geoadd key 精度 维度 member(就是店铺的id)
            for (Shop shop : value) { // 经纬度融合为了point
//                stringRedisTemplate.opsForGeo().add(key, new Point(shop.getX(), shop.getY()), shop.getId().toString());
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(), shop.getY())));
            }
            stringRedisTemplate.opsForGeo().add(key, locations);
        }
    }

    @Test
    void testIdWorker() {
        // 线程
        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println("id=" + id);
            }
        };
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        long end = System.currentTimeMillis();

    }

    @Resource
    private ShopServiceImpl service;

    @Resource
    private CacheClient cacheClient;

    /**
     * 预热
     *
     * @throws InterruptedException
     */
    @Test
    void test54564() throws InterruptedException {
//        service.saveShop2Redis(1L, 10L);
        Shop shop = shopService.getById(1);
        cacheClient.setWithLogiclExpire(CACHE_SHOP_KEY + 1L, shop, 10L, TimeUnit.SECONDS);
    }

}
