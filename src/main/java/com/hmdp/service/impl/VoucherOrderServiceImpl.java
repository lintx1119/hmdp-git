package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;   // 脚本
    static {    // 脚本初始化
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill_mq.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024); // 阻塞队列
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();  //异步处理线程池
    private IVoucherOrderService proxy;

//    //在类初始化之后执行，因为当这个类初始化好了之后，随时都是有可能要执行的
//    @PostConstruct
//    private void init(){
//        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
//    }
//
//    // 用于线程池处理的任务
//    // 当初始化完毕后，就会去从队列中去拿信息
//    private class VoucherOrderHandler implements Runnable{
//
//        @Override
//        public void run() {
//            while (true){
//                try {
//                    VoucherOrder order = orderTasks.take();
//                    // 开启了新线程，事务会失效
//                    proxy.handlerVoucherOrder(order);
//
//                } catch (InterruptedException e) {
//                    log.error("处理订单异常", e);
//                }
//            }
//        }
//    }
//
    @Transactional
    public void handlerVoucherOrder(VoucherOrder order){
        // 减库存
        seckillVoucherService.update().setSql("stock = stock - 1").eq("voucher_id", order.getVoucherId()).update();
        // 保存订单
        save(order);
    }
//
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), UserHolder.getUser().getId().toString()
//        );
//
//        // 判断结果
//        if (result.intValue() != 0){
//            if (result.intValue() == 1)    return Result.fail("库存不足");
//            else if (result.intValue() == 2) return Result.fail("该用户已经购买过了");
//        }
//
//        // 存放到阻塞队列
//        long orderId = redisIdWorker.nextId("order");
//        VoucherOrder voucherOrder = new VoucherOrder();
//        voucherOrder.setVoucherId(voucherId);    // 优惠券ID
//        voucherOrder.setUserId(UserHolder.getUser().getId());       // 用户ID
//        voucherOrder.setId(orderId);           // 订单ID
//
//        orderTasks.add(voucherOrder);
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        return Result.ok(orderId);
//    }

    // 使用基于stream的消息队列
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        // 执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),       // 优惠卷ID
                userId.toString(),          // 用户ID
                String.valueOf(orderId)     // 订单ID
        );

        // 判断结果
        if (result.intValue() != 0){
            if (result.intValue() == 1)    return Result.fail("库存不足");
            else if (result.intValue() == 2) return Result.fail("该用户已经购买过了");
        }

        proxy = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);
    }

    //在类初始化之后执行，因为当这个类初始化好了之后，随时都是有可能要执行的
    @PostConstruct
    private void init_mq(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler_MQ());
    }

    // 用于线程池处理的任务
    // 当初始化完毕后，就会去从队列中去拿信息
    private class VoucherOrderHandler_MQ implements Runnable{

        @Override
        public void run() {
            while (true){
                try {
                    // 获取消息队列中的订单消息, XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
                    );
                    // 判断消息是否获取成功
                    if (list == null || list.isEmpty()){
                        continue;
                    }
                    // 解析订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 下单
                    proxy.handlerVoucherOrder(voucherOrder);
                    // ACK确认, XACK stream.orders g1
                    stringRedisTemplate.opsForStream().acknowledge("stream.orders","g1", record.getId());
                } catch (Exception e) {
                    log.error("处理消息队列订单异常", e);
                    // 消息异常就会进入pending-list
                    while (true){
                        try {
                            // 获取pending-list中的订单消息, XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders 0
                            List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                                    Consumer.from("g1", "c1"),
                                    StreamReadOptions.empty().count(1),
                                    StreamOffset.create("stream.orders", ReadOffset.from("0"))
                            );
                            // 判断消息是否获取成功
                            if (list == null || list.isEmpty()){    // 说明没有pending-list没有异常消息了
                               break;
                            }
                            // 解析订单信息
                            MapRecord<String, Object, Object> record = list.get(0);
                            Map<Object, Object> value = record.getValue();
                            VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                            // 下单
                            proxy.handlerVoucherOrder(voucherOrder);
                            // ACK确认, XACK stream.orders g1
                            stringRedisTemplate.opsForStream().acknowledge("stream.orders","g1", record.getId());
                        } catch (Exception ex) {
                            log.error("处理pending-list订单异常", e);
                        }
                    }
                }
            }
        }
    }

    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 一人一单
        int count = query().eq("user_id", UserHolder.getUser().getId()).eq("voucher_id", voucherId).count();
        if (count > 0){
            return Result.fail("该用户已经购买过了");
        }

        // 5、扣减库存(结合乐观锁，防止超卖)
        boolean success = seckillVoucherService.update().setSql("stock = stock - 1").
                eq("voucher_id", voucherId).gt("stock", 0).update();
        if (!success){
            return Result.fail("库存不足!");
        }

        // 6、创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setVoucherId(voucherId);    // 优惠券ID
        voucherOrder.setUserId(UserHolder.getUser().getId());       // 用户ID
        voucherOrder.setId(redisIdWorker.nextId("order"));           // 订单ID
        save(voucherOrder);

        // 前端的 Number 类型无法精确表示这么大的数，导致精度丢失，从而出现了前端显示的订单ID和数据库中的订单ID不同的情况。
        return Result.ok(String.valueOf(voucherOrder.getId()));
    }

    // 使用synchronized来锁userId
    public Result seckillVoucher_synchronized(Long voucherId) {
        // 1、查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 2、判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀尚未开始！");
        }
        // 3、判断秒杀是否已经结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀已经结束！");
        }
        // 4、判断库存是否充足
        if (voucher.getStock() < 1) {
            // 库存不足
            return Result.fail("库存不足！");
        }

        // 一人一单，结合悲观锁
        Long userId = UserHolder.getUser().getId();

        // 在集群环境下仍然存在并发问题
        synchronized (userId.toString().intern()){
            // 获取代理对象（事务）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }

    }

    // 使用基于手动实现的redis的分布式锁来锁userId，仍然有一些缺陷
    public Result seckillVoucher_simpleRedisLock(Long voucherId) {
        // 1、查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 2、判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀尚未开始！");
        }
        // 3、判断秒杀是否已经结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀已经结束！");
        }
        // 4、判断库存是否充足
        if (voucher.getStock() < 1) {
            // 库存不足
            return Result.fail("库存不足！");
        }

        // 一人一单，结合悲观锁
        Long userId = UserHolder.getUser().getId();

        SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate, "order:" + userId);
        // 获取锁
        boolean isLock = lock.tryLock(1200);
        if (!isLock){
            // 返回错误或者重试
            return Result.fail("一个人不允许重复下单");
        }
        try {
            // 获取代理对象（事务）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            // 释放锁
            lock.unlock();
        }

    }

    // 使用redisson封装好的redis分布式锁来锁userId
    public Result seckillVoucher_redisson(Long voucherId) {
        // 1、查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 2、判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀尚未开始！");
        }
        // 3、判断秒杀是否已经结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀已经结束！");
        }
        // 4、判断库存是否充足
        if (voucher.getStock() < 1) {
            // 库存不足
            return Result.fail("库存不足！");
        }

        // 一人一单，结合悲观锁
        Long userId = UserHolder.getUser().getId();

        RLock lock = redissonClient.getLock("lock:order:" + userId);
        // 获取锁
        boolean isLock = lock.tryLock();
        if (!isLock){
            // 返回错误或者重试
            return Result.fail("一个人不允许重复下单");
        }
        try {
            // 获取代理对象（事务）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }
}
