package com.cinfly.dadaxiaopu.utils;


import com.cinfly.dadaxiaopu.dto.UserDTO;
import com.cinfly.dadaxiaopu.entity.User;
import org.springframework.web.servlet.HandlerInterceptor;


import javax.jws.soap.SOAPBinding;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class LoginInteceptor implements HandlerInterceptor {
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //1.获取用户,不存在直接返回
        if(UserHolder.getUser()==null)
        {
            response.setStatus(401);
            return false;
        }
        //放行
        return true;
    }

}
