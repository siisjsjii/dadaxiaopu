package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.constant.RedisConstants;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.JwtUtil;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private JwtUtil jwtUtil;

    @Override
    public Result sendCode(String phone) {
        //1.校验手机号
        if(RegexUtils.isPhoneInvalid(phone))
        {
            return Result.fail("手机号格式错误");
        }
        //3.符合,生成验证码,保存到redis
        String code = RandomUtil.randomNumbers(6);
        stringRedisTemplate.opsForValue().set(RedisConstants.CODE_KEY +phone,code, RedisConstants.CODE_TTL, TimeUnit.MINUTES);
        //发送验证码
        log.info("发送验证码成功，验证码：{}",code);
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm) {
        //1.校验手机号
        String phone=loginForm.getPhone();
        if(RegexUtils.isPhoneInvalid(phone))
        {
            return Result.fail("手机号格式错误");
        }

        //2.校验验证码
        String cacheCode =stringRedisTemplate.opsForValue().get(RedisConstants.CODE_KEY + phone);
        String code=loginForm.getCode();
        if(cacheCode==null||!code.equals(cacheCode))
        {
            return Result.fail("验证码错误");
        }
        //4.根据手机号查用户
        User user = lambdaQuery().eq(User::getPhone, phone).one();

        //5.用户不存在,创建新用户
        if(user==null)
        {
            user=createUserWithPhone(phone);
        }

       /* //6.保存用户信息到redis,将对象转换成map保存,并且设置token过期时间
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        String token = UUID.randomUUID().toString();
        String tokenKey= RedisConstants.USER_KEY+ token;
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(), CopyOptions.create()
                .setIgnoreNullValue(true)
                .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));

        stringRedisTemplate.opsForHash().putAll(tokenKey,userMap);
        stringRedisTemplate.expire(tokenKey, RedisConstants.USER_TTL,TimeUnit.MINUTES);*/
        //生成jwt返回给前端
        UserDTO userDTO=BeanUtil.copyProperties(user, UserDTO.class);
        String token = jwtUtil.createUserToken(userDTO);
        return Result.ok(token);
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName("user_"+RandomUtil.randomString(10));
        save(user);
        return user;
    }

    @Override
    public Result sign() {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //获取日期
        LocalDateTime now = LocalDateTime.now();
        String month = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key=RedisConstants.USER_SIGN_KEY+userId+month;
        //签到
        int day=now.getDayOfMonth();
        stringRedisTemplate.opsForValue().setBit(key,day-1,true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        //1.获取本月截取到今天为止的签到记录
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //获取日期
        LocalDateTime now = LocalDateTime.now();
        String month = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key=RedisConstants.USER_SIGN_KEY+userId+month;
        int day=now.getDayOfMonth();
        //返回十进制数字
        List<Long> result = stringRedisTemplate.opsForValue().bitField(key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(day))
                        .valueAt(0));

        if(CollUtil.isEmpty(result))
        {
            return Result.ok(0);
        }
        //循环遍历和1与运算,结果为1签到数加一
        Long number = result.get(0);
        if(number==null||number==0)
        {
            return Result.ok(0);
        }
        int count=0;
        while(true)
        {
            if((number&1)==0)
            {
                break;
            }
            else
            {
                count++;
            }
            number>>>=1;
        }

        return Result.ok(count);
    }
}
