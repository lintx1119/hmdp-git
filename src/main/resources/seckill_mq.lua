-- 参数列表
local voucherId = ARGV[1]     -- 优惠券ID，判断库存是否充足
local userId = ARGV[2]     -- 用户ID，判断一人一单
local orderId = ARGV[3]     -- 订单ID，发送到消息队列
-- 数据KEY
local stockKey = 'seckill:stock:' .. voucherId       -- 库存key
local orderKey = 'seckill:order:' .. voucherId       -- 订单key

-- 脚本业务
-- 1、判断库存是否充足
if(tonumber(redis.call('get',stockKey)) <= 0) then
    return 1        -- 库存不足标识
end

-- 2、判断用户是否下单
if(redis.call('sismember', orderKey, userId) == 1) then
    return 2    -- 重复下单标识
end

-- 3、扣库存
redis.call('incrby', stockKey, -1)
-- 4、下单
redis.call('sadd', orderKey, userId)
-- 5、发送消息到队列当中 XADD stream.orders * k1 v1 k2 v2 ...
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)

return 0