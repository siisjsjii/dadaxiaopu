/*
package com.cinfly.dadaxiaopu.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import com.cinfly.dadaxiaopu.constant.RedisConstants;
import com.cinfly.dadaxiaopu.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RefreshTokenInteceptor implements HandlerInterceptor {
    private final StringRedisTemplate stringRedisTemplate;
    public RefreshTokenInteceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
      //1.获取请求头中的token
        String token = request.getHeader("authorization");
        if(token==null)
        {
            //放行给下一个拦截器
            return true;
        }
        //获取用户
        String userKey= RedisConstants.USER_KEY+token;
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(userKey);
        if(CollUtil.isEmpty(userMap))
        {
            return true;
        }
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
        UserHolder.saveUser(userDTO);
        //刷新token有效期
        stringRedisTemplate.expire(userKey, RedisConstants.USER_TTL, TimeUnit.MINUTES);
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        UserHolder.removeUser();
    }
}
*/
