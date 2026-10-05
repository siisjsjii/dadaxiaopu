package com.hmdp.utils;

import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class JwtLoginInterceptor implements HandlerInterceptor {

    private final JwtUtil jwtUtil;

    public JwtLoginInterceptor(JwtUtil jwtUtil) {
        this.jwtUtil = jwtUtil;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        String token = request.getHeader("authorization");
        if (StrUtil.isNotBlank(token)) {
            UserDTO userDTO = jwtUtil.parseToken(token);
            if (userDTO != null) {
                UserHolder.saveUser(userDTO);
            }
        }
        else
        {
            return false;//未登录
        }
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {
        UserHolder.removeUser();
    }
}