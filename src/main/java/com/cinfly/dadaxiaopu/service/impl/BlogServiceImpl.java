package com.cinfly.dadaxiaopu.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.cinfly.dadaxiaopu.constant.RedisConstants;
import com.cinfly.dadaxiaopu.dto.Result;
import com.cinfly.dadaxiaopu.dto.ScrollResult;
import com.cinfly.dadaxiaopu.dto.UserDTO;
import com.cinfly.dadaxiaopu.entity.Blog;
import com.cinfly.dadaxiaopu.entity.Follow;
import com.cinfly.dadaxiaopu.entity.User;
import com.cinfly.dadaxiaopu.mapper.BlogMapper;
import com.cinfly.dadaxiaopu.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.cinfly.dadaxiaopu.service.IFollowService;
import com.cinfly.dadaxiaopu.service.IUserService;
import com.cinfly.dadaxiaopu.utils.SystemConstants;
import com.cinfly.dadaxiaopu.utils.UserHolder;
import com.zaxxer.hikari.util.IsolationLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.swing.text.StyledEditorKit;
import java.util.ArrayList;
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
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IFollowService followService;

    private static final String BLOG_LIKED_KEY="blog:liked:";

    @Override
    public Result saveBlog(Blog blog) {
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        boolean result = save(blog);
        if(!result)
        {
            return Result.fail("发布失败");
        }
        //查询笔记作者的所有粉丝
        List<Follow> list = followService.lambdaQuery().eq(Follow::getFollowUserId, user.getId()).list();
        if(CollUtil.isEmpty(list))
        {
            return Result.ok(blog.getId());
        }
        //推送笔记给所有粉丝
        for (Follow follow : list) {
            //获取粉丝id
            Long userId = follow.getUserId();
            stringRedisTemplate.opsForZSet().add(RedisConstants.FEEDS_KEY+userId,blog.getId().toString(), System.currentTimeMillis());

        }


        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogById(Long id) {
        Blog blog = getById(id);
        if(blog==null)
        {
            return Result.fail("日志不存在");
        }
        queryBlogUser(blog);
        blog.setIsLike(isBlogLiked(id));
        return Result.ok(blog);
    }
    private boolean isBlogLiked(Long id) {
        // 获取当前登录用户
        UserDTO user = UserHolder.getUser();
        if(user==null)
        {
            return false;
        }
        Long userId = user.getId();
        // 判断当前用户是否已经liked
        String key = "blog:liked:" + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        return score != null;
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
        blog.setIsLike(isBlogLiked(blog.getId()));
    }

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(this::queryBlogUser);
        return Result.ok(records);
    }

    @Override
    public Result likeBlog(Long id) {
       //判断当前用户有无点赞
        Long userId = UserHolder.getUser().getId();
        String key="blog:liked:"+id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        //未点赞,数据库点赞数+1,redis存入当前用户
        if(score==null)
        {
            boolean success = lambdaUpdate().setSql("liked = liked + 1").eq(Blog::getId, id).update();
            if(success)
            {
                stringRedisTemplate.opsForZSet().add(key,userId.toString(),System.currentTimeMillis());
            }
        }
        //已经点赞,数据库点赞数-1,redis删除当前用户
        else
        {
            boolean success = lambdaUpdate().setSql("liked = liked - 1").eq(Blog::getId, id).update();
            if(success)
            {
                stringRedisTemplate.opsForZSet().remove(key,userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        //redis查询排行榜前十
        String key=BLOG_LIKED_KEY+id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 5);
        if(CollUtil.isEmpty(top5))
        {
            return Result.ok();
        }
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = CollUtil.join(ids, ",");
        List<UserDTO> userDTOS = userService.query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list()
                .stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());
        return Result.ok(userDTOS);
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
      //查询当前用户
        UserDTO user = UserHolder.getUser();
        Long userId = user.getId();
        String key = RedisConstants.FEEDS_KEY+userId;
        //查询收件箱
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        if(CollUtil.isEmpty(typedTuples))
        {
            return Result.ok();
        }

        //解析数据,bolgId,mintime,offset
        List<Long>ids=new ArrayList<>(typedTuples.size());
        long minTime=0;
        int offsets=1;
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            //获取id
            String blogId = typedTuple.getValue();
            if(typedTuple.getScore()==minTime)
            {
                offsets++;
            }
            else
            {
                minTime = typedTuple.getScore().longValue();
                offsets=1;
            }
            ids.add(Long.valueOf(blogId));

        }
        //根据id查询blog
        String idsStr = StrUtil.join(",", ids);
        List<Blog> blogs = lambdaQuery().in(Blog::getId, ids).last("ORDER BY FIELD(id," + idsStr + ")").list();
        //查询是否被点赞
        blogs.forEach(this::queryBlogUser);
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setOffset(offsets);
        scrollResult.setMinTime(minTime);

        //封装返回
        return Result.ok(scrollResult);

    }
}
