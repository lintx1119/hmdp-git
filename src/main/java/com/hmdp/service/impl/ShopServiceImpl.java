package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {
        // 缓存穿透
        Shop shop = queryWithPassThrough(id);
        // 解决缓存击穿（互斥锁）
        // Shop shop = queryWithPassThrough(id);
        // 解决缓存击穿（逻辑过期）
        // Shop shop = queryWithLogicalExpire(id);

        if (shop == null){
            return Result.fail("店铺不存在！");
        }

        return Result.ok(shop);
    }

    // 简单的给商户信息 添加/使用 缓存（对于低一致性要求）
    public Result simpleCache(long id){
        // 1、从redis查询商户缓存
        String shopCache = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        if (StrUtil.isNotBlank(shopCache)){
            Shop shop = JSONUtil.toBean(shopCache, Shop.class);
            return Result.ok(shop);
        }

        // 2、redis中没有,则查询数据库
        Shop shop = getById(id);
        if (shop == null){
            return Result.fail("商户不存在!");
        }

        // 3、将数据库中的信息存入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop));
        // 高一致性要求，设置超时剔除时间。再结合更新数据删除缓存操作！
        // stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);

        return Result.ok(shop);
    }

    // 更新商户信息（对于高一致性要求）
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null){
            return Result.fail("店铺id不能为空！");
        }
        //更新数据库
        updateById(shop);
        //删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);

        return Result.ok();
    }

    // 解决缓存穿透
    public Shop queryWithPassThrough(Long id){
        // 从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        // isNotBlank只有存在字符串时 ==> true, 否则(如null、""、"\t\n") ==> false
        if (StrUtil.isNotBlank(shopJson)){
            Shop shopCache = JSONUtil.toBean(shopJson, Shop.class);
            return shopCache;
        }

        // 判断命中的是否是空值-->""
        // 这段代码的逻辑: 之前判断过是否为null/""了, 现在只要判断不为null, 即""
        if (shopJson != null){
            return null;
        }

        // redis缓存中不存在
        Shop shop = getById(id);
        if (shop == null){
            // 解决缓存穿透，返回空对象
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 若存在，则存入redis缓存，并设置超时时间
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + shop.getId(), JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    // 解决缓存击穿(互斥锁)
    public Shop queryWithMutex(Long id){
        // 从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        // isNotBlank只有存在字符串时 ==> true, 否则(如null、""、"\t\n") ==> false
        if (StrUtil.isNotBlank(shopJson)){
            Shop shopCache = JSONUtil.toBean(shopJson, Shop.class);
            return shopCache;
        }

        // 判断命中的是否是空值
        if (shopJson != null){
            return null;
        }
        Shop shop = null;

        try {
            // 实现缓存重建
            // 获取互斥锁
            boolean isLock = tryLock(LOCK_SHOP_KEY + id);
            if (!isLock){   //失败
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            // 成功
            shop = getById(id);
            Thread.sleep(200);  // 模拟重建时候的延时
            // redis缓存中不存在
            if (shop == null){
                // 解决缓存穿透，返回空对象
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 若存在，则存入redis缓存，并设置超时时间
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + shop.getId(), JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //释放互斥锁
            unlock(LOCK_SHOP_KEY + id);
        }

        return shop;
    }

    // 解决缓存击穿(逻辑过期)
    private static final ExecutorService es = Executors.newFixedThreadPool(10); // 创建一个固定大小的线程池，其中最多允许10个线程同时运行，用于执行异步任务。
    public Shop queryWithLogicalExpire(Long id){
        // 从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        // 未命中，返回空
        if (StrUtil.isBlank(shopJson)){
            return null;
        }

        // 命中，判断缓存数据是否过期
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 没过期，直接返回
        if (expireTime.isAfter(LocalDateTime.now())){
            return shop;
        }

        // 过期，获取锁
        boolean isLock = tryLock(LOCK_SHOP_KEY + id);

        // 拿到锁，开启独立线程重建缓存
        if (isLock){
            es.submit(()->{
                try {
                    // 重建缓存
                    saveShop2Redis(id, 20L);
                    Thread.sleep(200);      // 模拟缓存重建需要很久
                } catch (Exception e){
                    throw new RuntimeException(e);
                } finally {
                    unlock(LOCK_SHOP_KEY + id);
                }
            });
        }
        // 返回商铺信息，无论有没拿到锁
        return shop;
    }

    // 获取锁, redis实现互斥锁(setnx)
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.MINUTES);
        return BooleanUtil.isTrue(flag);
    }

    // 释放锁
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    // 预热，添加数据到redis
    public void saveShop2Redis(Long id, Long expireSeconds){
        Shop shop = getById(id);
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 判断是否根据坐标查询
        if (x == null || y == null){
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        // 分页参数计算
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        // 查询redis，按照距离排序、分页。结果：shopId，distance
        // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(
                SHOP_GEO_KEY + typeId,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end));
        if (results == null)
            return Result.ok();
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> content = results.getContent();

        if (content.size() <= from)     // 没有下一页了
            return Result.ok();

        // 截取从from到end的部分
        List<Long> ids = new ArrayList<>();
        Map<String, Distance> distanceMap = new HashMap<>();
        content.stream().skip(from).forEach(result -> {
            // 获取店铺id
            String shopId = result.getContent().getName();
            ids.add(Long.valueOf(shopId));
            // 获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopId, distance);
        });

        // 根据店铺id查询shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shopList = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shopList) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        return Result.ok(shopList);
    }

}
