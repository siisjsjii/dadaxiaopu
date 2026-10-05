package com.hmdp.config;

import com.hmdp.utils.JwtLoginInterceptor;
import com.hmdp.utils.JwtUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.annotation.Resource;


@Configuration
public class MvcConfig implements WebMvcConfigurer {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private JwtUtil jwtUtil;
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        //登录拦截器
        registry.addInterceptor(new JwtLoginInterceptor(jwtUtil))
                .excludePathPatterns("/user/login",
                "/doc.html",
                "/webjars/**",
                "/swagger-resources/**",
                "/v2/api-docs",
                        "/user/code");
    }
}
