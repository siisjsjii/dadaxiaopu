package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.hmdp.constant.RedisConstants;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.apache.ibatis.io.ResolverUtil;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IUserService userService;
    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        Long userId=UserHolder.getUser().getId();
        if (isFollow) {
            //关注,新增数据
            Follow follow = new Follow();
            follow.setFollowUserId(followUserId);
            follow.setCreateTime(LocalDateTime.now());
            follow.setUserId(userId);
            boolean succerss = save(follow);
            if(succerss)
            {
                //关注成功,将被关注者加入redis关注列表
                stringRedisTemplate.opsForSet().add(RedisConstants.FOLLOW_KEY+userId,followUserId.toString());
            }
        }
        else
        {
            //取关,删除数据
            boolean success = remove(new LambdaUpdateWrapper<Follow>().eq(Follow::getUserId, userId).eq(Follow::getFollowUserId, followUserId));
            if(success)
            {
                //取关成功,将取消关注者从redis关注列表中移除
                stringRedisTemplate.opsForSet().remove(RedisConstants.FOLLOW_KEY+userId,followUserId.toString());
            }

        }
        return Result.ok();
    }

    @Override
    public Result followOrNot(Long followUserId) {
        Long userId=UserHolder.getUser().getId();
        Integer count = lambdaQuery().eq(Follow::getUserId, userId).eq(Follow::getFollowUserId, followUserId).count();
        return Result.ok(count>0);
    }

    @Override
    public Result common(Long id) {
        Long userId = UserHolder.getUser().getId();
        String key1=RedisConstants.FOLLOW_KEY+userId;
        String key2=RedisConstants.FOLLOW_KEY+id;
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key1, key2);
        //根据交集id查询用户
        if(CollUtil.isEmpty(intersect))
        {
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        List<User> users = userService.listByIds(ids);
        List<UserDTO> userDTOs = users.stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());
       return Result.ok(userDTOs);
    }
}
