package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    // 我们要查询优惠券 所以我们要调用优惠券的service  查询秒杀的
    @Resource
    private ISeckillVoucherService iSeckillVoucherService;

    // 我们订单id的生成器
    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    // 注入我们新的锁的形式  阻塞式的
    @Autowired
    private RedissonClient redissonClient;

    // 执行lua脚本的静态代码块
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    // 创建阻塞队列(这个因为有范围限制所以我们就不使用阻塞队列来实现了)
//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    // 创建线程池(单线程的)
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct //这个就是当这个类初始化后就执行
    private void init() {
        // 提交我们的线程任务(然后我们的run方法就执行了)
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private static final String queueName = "stream.order";

    // 消息队列的线程
    private class VoucherOrderHandler implements Runnable {
        //      注意要在秒杀活动开始之前开启这个消息队列(应该在我们这个类初始化的时候就开始任务)

        @Override
        public void run() {
            while (true) {
                try {
                    // 1.获取(消息)队列中的订单信息 xread group g1 c1 count 1 block 2000 streams streams.order >
                    List<MapRecord<String, Object, Object>> msg = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.from(">"))
                    );

                    // 2.判断消息是否获取成功
                    if (msg == null || msg.isEmpty()) {
                        // 2.1失败 就继续等待
                        continue;
                    }

                    // 2.2 有消息就下单(表示有消息)
                    // 创建订单(解析消息)
                    MapRecord<String, Object, Object> record = msg.get(0);
                    Map<Object, Object> values = record.getValue();
                    // 定义一个方法来实现我们的订单的创建
                    // 这个就是转换为我们的订单  true 表示遇到异常就忽略
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    // 3.创建订单
                    handVoucherOrder(voucherOrder);

                    // 4.下单完成后还要进行ack的确认
                    // 从record里面可以得到每一个的id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    // 在异常里面 就要弄我们的pending-list来获取当时发生异常没有处理的订单
                    log.error("处理订单异常", e);
                    handPendingList();

                }
            }
        }
    }

    private void handPendingList() {
        while (true) {

            try {
                // 1.获取我们的pending-list里面异常的订单  后面记得变为0
                List<MapRecord<String, Object, Object>> msg = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", "c1"),
                        StreamReadOptions.empty().count(1), // 这里就不需要我们的block因为异常里面就一定有消息
                        StreamOffset.create(queueName, ReadOffset.from("0"))
                );
                // 注意这里我们改成了0 就表示一直读取 知道我们为空为止

                // 2.获取我们的信息
                if (msg == null || msg.isEmpty()) {
                    // 2.1 如果pending-list没有信息了就退出去
                    break;
                }
                // 2.2 表示有信息
                // 3.解析我们的信息
                MapRecord<String, Object, Object> record = msg.get(0);
                Map<Object, Object> values = record.getValue();
                // 3.1 得到我们的订单信息
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                // 4. 创建订单
                handVoucherOrder(voucherOrder);
                // 5. 进行ack
                stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());

            } catch (Exception e) {
                // 表示我们获取pending-list还出现了异常 那么这个还会流在pending-list里面
                log.error("处理pending-list订单异常", e);
                // 所以我们的就还要重新执行一遍(当走了几遍发现还是报错的话就设置一下就可以认为修改)
//                handPendingList();
                // 注意 你不用写上面的方法因为 我们这个while 是个死循环

                // 但是如果你怕一直处于这个状况的话 就可以进行线程的休眠
                try {
                    Thread.sleep(20);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }


    // 创建线程任务(因为这个是我们阻塞队列 的线程 但是我们没有用了用的是(消息队列)来实现的)
    /*private class VoucherOrderHandler implements Runnable {
        //      注意要在秒杀活动开始之前开启这个阻塞队列(应该在我们这个类初始化的时候就开始任务)
        @Override
        public void run() {
            while (true) {
                try {
                    // 获取队列中的订单信息
                    VoucherOrder voucehrOrder = orderTasks.take();
                    // 创建订单
                    // 定义一个方法来实现我们的订单的创建
                    handVoucherOrder(voucehrOrder);

                } catch (Exception e) {
                    log.error("处理订单异常", e);

                }
            }
        }
    }*/

    private IVoucherOrderService proxy;

    @Transactional
    public void handVoucherOrder(VoucherOrder voucehrOrder) {
        Long userId = voucehrOrder.getUserId();
        // 创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        // 获取锁
        boolean flag = lock.tryLock();

        if (!flag) {
            // 获取失败
            log.error("不允许重复下单");
            return;
        }
        try {
            // 注意这个是子线程拿不到代理对象的 我们可以定义一个全部变量来实现
            // 然后在主线程里面就可以得到我们的代理对象了
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            proxy.createVoucherOrder(voucehrOrder);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }


    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        // 这个classpathresouce就是直接在我们的resource里面去找
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        // 返回值
        SECKILL_SCRIPT.setResultType(Long.class);

    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户id
        Long userId = UserHolder.getUser().getId();
//        System.out.println(userId);
        // 获取订单id
        // 注意这个是小写的long 因为我们当时怕拆箱的时候出现问题
        long orderId = redisIdWorker.nextId("order");


        // 1.执行lua脚本
//        Collections.emptyList(),传的空的因为我们的lua脚本里面是没有key的参数的
//        但是你又不能传null进去
        // 就是发消息(stream.order)
        Long flag = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );


        // 2.判断是否为0
        // 先将long转为 int
        int r = flag.intValue();
        if (r != 0) {
            // 2.1不为0 没有购买资格
            // 表示不能购买
            if (r == 1) {
                return Result.fail("库存不足，无法购买");
            } else if (r == 2) {
                return Result.fail("你已经购买，不能重复下单");
            }

        }

/*

        // 2.2 为0 有购买的资格  把下单信息保存到阻塞队列里面去
        // TODO 保存到阻塞队列
//        就是我们的VoucherOreder的类
        VoucherOrder voucherOrder = new VoucherOrder();
        // 2.3订单id
        long orederId = redisIdWorker.nextId("oreder");
        voucherOrder.setId(orederId);

        // 2.4用户id
        voucherOrder.setUserId(userId);

        // 2.5代金券id
        voucherOrder.setVoucherId(voucherId);

        // 2.6 将订单放入阻塞队列里面去
        orderTasks.add(voucherOrder);
*/

        // 3. 获取代理对象(这个就是主线程的)
        proxy = (IVoucherOrderService) AopContext.currentProxy();


        // 4.返回id给用户
        return Result.ok(orderId);
    }


    /*@Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户id
        Long userId = UserHolder.getUser().getId();
        System.out.println(userId);


        // 1.执行lua脚本
//        Collections.emptyList(),传的空的因为我们的lua脚本里面是没有key的参数的
//        但是你又不能传null进去
        Long flag = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString()
        );


        // 2.判断是否为0
        // 先将long转为 int
        int r = flag.intValue();
        if (r != 0) {
            // 2.1不为0 没有购买资格
            // 表示不能购买
            if (r == 1) {
                return Result.fail("库存不足，无法购买");
            } else if (r == 2) {
                return Result.fail("你已经购买，不能重复下单");
            }

        }

        // 订单id
        long orderId = redisIdWorker.nextId("order");

        // 2.2 为0 有购买的资格  把下单信息保存到阻塞队列里面去
        // TODO 保存到阻塞队列
//        就是我们的VoucherOreder的类
        VoucherOrder voucherOrder = new VoucherOrder();
        // 2.3订单id
        long orederId = redisIdWorker.nextId("oreder");
        voucherOrder.setId(orederId);

        // 2.4用户id
        voucherOrder.setUserId(userId);

        // 2.5代金券id
        voucherOrder.setVoucherId(voucherId);

        // 2.6 将订单放入阻塞队列里面去
        orderTasks.add(voucherOrder);

        // 3. 获取代理对象(这个就是主线程的)
        proxy = (IVoucherOrderService) AopContext.currentProxy();


        // 4.返回id给用户
        return Result.ok(orderId);
    }*/


    /*@Override
    public Result seckillVoucher(Long voucherId) {

        // 1.id传过来我们要进行查询
        SeckillVoucher voucher = iSeckillVoucherService.getById(voucherId);

        // 2.判断秒杀是否开始
        // 就是取出里面的时间
        LocalDateTime beginTime = voucher.getBeginTime();
        // isAfter就是时间大于的意思
        if (beginTime.isAfter(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀尚未开始");
        }

        // 3.判断秒杀是否结束
        LocalDateTime endTime = voucher.getEndTime();
        if (endTime.isBefore(LocalDateTime.now())) {
            // 秒杀已经结束
            return Result.fail("秒杀已经结束");
        }

        // 4.判断库存是否存足
        Integer stock = voucher.getStock();
        // 这个就是我们的库存
        if (stock <= 0) {
            // 表示库存不足
            return Result.fail("库存不足");
        }

        // 在这进行上锁
        Long userId = UserHolder.getUser().getId();

        // 创建锁对象
//        SampleRedisLock lock = new SampleRedisLock(stringRedisTemplate, "order:" + userId);
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        // 现在就是失败不等待
        boolean isLock = lock.tryLock();
        // 判断是否获取锁成功
        if (!isLock) {
            // 获取失败，返回错误信息
            return Result.fail("不能重复下单");

        }
//        synchronized (userId.toString().intern()) { 这个不适用多主机
        // 但是我们这个还存在事务的问题()
        // 这个this 是VoucherOrderServiceImpl的 但是他没有事务
        // 我们要弄下面的对象才行
        //
        // 拿到我们的代理对象
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            // 释放锁
            lock.unlock();
        }
//        }


    }*/

    // 传整个订单对象
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 不同用户加不同锁  同一个用户 加同一个锁 就可以实现


        // 得到用户id(因为是异步的 所以只能通过订单来获取了)
        Long userId = voucherOrder.getUserId();


        // 使用关键字的形式来创建锁
        // 就是什么喃 每次进来一个新的用户id就加锁 并且只是给自己的userid加锁
        // 然后我们的新的名字来也加锁就你自己锁了后你的另外的账户还要登录也不行 就要等
        // 你这个操作完了就可以了
        // 我们这个intern有什么用喃 因为 我们的tostring()会new一个新的对象
        // 那么我们即使是相同的名字 也会被定义为不同的名字
        // 这个intern就表示 只看你的值 不看什么对象等等 就是去掉你的属性
        // 然后就可以实现如果有重复的用户进来就可以实现 锁

//        synchronized (userId.toString().intern()) {
        // 但是我们这里也有一个问题 就是我们的事务提交等你这个方法执行完了才提交的
        // 但是你你这锁你线程一直等待的 可能会有漏洞(你还没提交事务的时候这时候新的线程
        // 进来了那么就会出现这个问题  所以应该加在 我们的方法李才行 等我们的事务提交了才
        // 释放我们的锁)


        // 5. 一人一单  就是一个人只能买一张优惠券
        // 5.1查询我们的用户id 和 我们这个优惠券id 就可以判断这个用户是否重复卖了 优惠券
        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();

        // 5.2判断订单是否存在
        if (count > 0) {
            // 5.3 如果如在直接返回
            log.error("你已经购买,每人限购一份!");
            return;
        }

        // 发现确实没有买过 就减少库存

        // 6.减少库存
        // 执行到这里就说能能够秒杀
        // 这个是我们的mybatisplus的写法  就是跟新 然后设置我们的stock减去一个
        // 后面的eq 就是我们的 where id = ? 就是这个语法
//        eq("stock",voucher.getStock()) 加了这个语句 实现我们的乐观锁 就是以库存的形式来判断

        // 如果连个同时发生 a买是 库存为 10 b买是库存也是10 但是a快一步 那么我们的stock 就要减一
        // b买是就要比较当时存的stock和我们数据库的stock是否一样 不一样就买一下一个 他的就自动减一  只要他在买的时候与数据库对比 是否>0就可以买  就这么简单
        boolean flag = iSeckillVoucherService.update().setSql("stock = stock - 1").eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0).update();
        if (!flag) {
            // 表示扣减失败
            log.error("库存不足");
            return;

        }


        // 不存在在创建订单


        /*// 7.创建订单
//        就是我们的VoucherOreder的类
        VoucherOrder voucherOrder = new VoucherOrder();
        // 7.1订单id
        // 我们当时使用的是id的生成器
        long orederId = redisIdWorker.nextId("oreder");
        voucherOrder.setId(orederId);

        // 7.2用户id
        // 这个就是我们有用户拦截器 里面有我们的用户id
        // 相当于在userDTO里面

        voucherOrder.setUserId(userId);

        // 7.3代金券id
        voucherOrder.setVoucherId(voucherId);*/

        // 8.写入数据库
        save(voucherOrder);

        // 9.返回订单id
        // 返回到我们的页面上
        // 现在也不用返回id了 是异步的
//        return Result.ok();
//        }
    }
}
