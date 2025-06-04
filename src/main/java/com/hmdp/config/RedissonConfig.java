package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author 林太翔
 * @date 2025/1/26 19:57
 */
@Configuration
public class RedissonConfig {

    @Bean
    public RedissonClient redissonClient(){

        Config config = new Config();
        // ↓ 单节点的地址，config.useClusterServers()添加集群地址
        config.useSingleServer().setAddress("redis://8.138.1.221:6379").setPassword("root");

        return Redisson.create(config);
    }
}
