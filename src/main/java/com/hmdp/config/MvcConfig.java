package com.hmdp.config;

import com.hmdp.utils.LoginInterceptor;
import com.hmdp.utils.RefreshTokenInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.annotation.Resource;

@Configuration
public class MvcConfig implements WebMvcConfigurer {

    // MvcConfig类上面加了@Configuration注解，这个类通过spring创建，因此可以注入stringRedisTemplate

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // LoginInterceptor拦截ThreadLocal中不存在的请求
        registry.addInterceptor(new LoginInterceptor()).excludePathPatterns(
                "/user/login",
                "/user/code",
                "/blog/hot",
                // 有关店铺的信息和是否登录无关
                "/shop/**",
                "shop-type/**",
                // 上传与是否登录无关
                "/upload/**",
                // 优惠券信息
                "/voucher/**"
        ).order(1);
        // RefreshTokenInterceptor拦截所有请求
        registry.addInterceptor(new RefreshTokenInterceptor(stringRedisTemplate))
                // order()控制拦截器执行顺序，要先执行RefreshTokenInterceptor
                // 再执行LoginInterceptor，order越大执行优先级越高
                .addPathPatterns("/**").order(0);
    }
}
