package com.hmdp.config;

import com.hmdp.controller.interceptor.LoginInterceptor;
import com.hmdp.controller.interceptor.RefreshTokenInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * @author 林太翔
 * @date 2024/8/22 15:08
 */
@Configuration
public class SpringMvcConfig implements WebMvcConfigurer {

    @Autowired
    private LoginInterceptor loginInterceptor;
    @Autowired
    private RefreshTokenInterceptor refreshTokenInterceptor;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // token刷新拦截器
        registry.addInterceptor(refreshTokenInterceptor).addPathPatterns("/**");   // 可以后面加order(0 or 1 or...)指定顺序
        // 登录拦截器
        registry.addInterceptor(loginInterceptor).excludePathPatterns(
                "/user/code", "/user/login",
                "/blog/hot",
                "/shop/**",
                "/shop-type/**",
                "/upload/**",
                "/voucher/**"
        );
    }
}
