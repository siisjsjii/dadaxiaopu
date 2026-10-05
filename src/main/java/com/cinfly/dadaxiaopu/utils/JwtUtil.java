package com.cinfly.dadaxiaopu.utils;

import cn.hutool.jwt.JWT;
import cn.hutool.jwt.JWTPayload;
import cn.hutool.jwt.JWTUtil;
import cn.hutool.jwt.JWTValidator;
import com.cinfly.dadaxiaopu.dto.UserDTO;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Date;

@Component
public class JwtUtil {

    @Value("${hmdp.jwt.secret}")
    private String secret;

    @Value("${hmdp.jwt.ttl-minutes}")
    private Long ttlMinutes;

    public String createUserToken(UserDTO userDTO) {
        Date now = new Date();
        return JWT.create()
                .setPayload("id", userDTO.getId())
                .setPayload("nickName", userDTO.getNickName())
                .setPayload("icon", userDTO.getIcon())
                .setIssuedAt(now)
                .setExpiresAt(new Date(now.getTime() + ttlMinutes * 60 * 1000))
                .setKey(secret.getBytes(StandardCharsets.UTF_8))
                .sign();
    }

    /**
     * 解析并校验 token,签名错误或已过期返回 null
     */
    public UserDTO parseToken(String token) {
        try {
            if (!JWTUtil.verify(token, secret.getBytes(StandardCharsets.UTF_8))) {
                return null;
            }
            JWTValidator.of(token).validateDate(new Date());
            JWTPayload payload = JWTUtil.parseToken(token).getPayload();
            UserDTO userDTO = new UserDTO();
            // 注意：不能写成 (Long) payload.getClaim("id")。
            // Hutool 解析 JSON 数字时，能塞进 int 的值会解析成 Integer，
            // 直接强转 Long 会抛 ClassCastException —— 而它会被下面的 catch 吞掉，
            // token 因此被判为无效，最终表现为下游 UserHolder.getUser().getId() 空指针 500。
            // 统一经 toString() 再转，与具体数字类型无关。
            userDTO.setId(toLong(payload.getClaim("id")));
            userDTO.setNickName((String) payload.getClaim("nickName"));
            userDTO.setIcon((String) payload.getClaim("icon"));
            return userDTO;

        } catch (Exception e) {

            return null;
        }
    }

    private Long toLong(Object value) {
        return value == null ? null : Long.valueOf(value.toString());
    }
}