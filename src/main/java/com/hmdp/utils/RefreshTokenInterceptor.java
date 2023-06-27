package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 登录拦截器, 用于校验所有的登录是否符合要求，要实现HandlerInterceptor接口
 * <p>
 * HandlerInterceptor 共有三个方法，
 * preHandle 前置拦截
 * postHandle 在 Controller 执行之后
 * afterCompletion 在渲染之后，返回给用户之前
 */
public class RefreshTokenInterceptor implements HandlerInterceptor {

    // 这里不能使用@Resource注解进行注入，只能使用构造函数注入
    // 因为这个类是我们手动通过new创建出来的，不是由spring创建的

    private StringRedisTemplate stringRedisTemplate;

    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 1.获取请求头中的token
        String token = request.getHeader("authorization");
        if (StrUtil.isBlank(token)) {
            // 如果发现为空，则直接放行
            return true;
        }

        // 2.基于token获取redis中的用户
        String key = RedisConstants.LOGIN_USER_KEY + token;
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(key);

        // 3.判断用户是否存在
        if (userMap.isEmpty()) {
            // 4.用户不存在，则拦截， 返回 401 状态码， 表示未授权
            response.setStatus(401);
            return false;
        }

        // 注意:存储到redis时，是以hashMap的方式存进去的，因此取出来的也是hashMap形式
        // 5.将查询到的Hash数据转换为UserDTO对象
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
        // 6.用户存在，则保存用户信息到ThreadLocal，保证线程安全
        UserHolder.saveUser(userDTO);
        // 7.刷新token有效期
        stringRedisTemplate.expire(key, RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);
        // 8.放行
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        // 移除用户
        UserHolder.removeUser();
    }
}
