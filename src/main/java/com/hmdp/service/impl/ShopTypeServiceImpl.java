package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result getAll() {
        String shopTypeListJson = stringRedisTemplate.opsForValue().get("cache:shoptype");

        if (StrUtil.isNotBlank(shopTypeListJson)){
            List<ShopType> shopTypeListCache = JSONUtil.toList(shopTypeListJson, ShopType.class);
            return Result.ok(shopTypeListCache);
        }

        List<ShopType> shopTypeList = query().orderByAsc("sort").list();
        if (shopTypeList.isEmpty()){
            return Result.fail("暂无商铺列表!");
        }
        String jsonStr = JSONUtil.toJsonStr(shopTypeList);
        stringRedisTemplate.opsForValue().set("cache:shoptype", jsonStr);

        return Result.ok(shopTypeList);
    }
}
