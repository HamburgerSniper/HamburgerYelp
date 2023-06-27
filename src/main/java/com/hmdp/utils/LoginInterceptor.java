package com.hmdp.utils;

import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * 登录拦截器, 用于校验所有的登录是否符合要求，要实现HandlerInterceptor接口
 * <p>
 * HandlerInterceptor 共有三个方法，
 * preHandle 前置拦截
 * postHandle 在 Controller 执行之后
 * afterCompletion 在渲染之后，返回给用户之前
 */
public class LoginInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 1.判断是否需要拦截(ThreadLocal中是否有用户)
        if (UserHolder.getUser() == null) {
            // 没有，需要拦截
            response.setStatus(401);
            return true;
        }
        return true;
    }
}
