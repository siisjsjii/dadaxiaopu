package com.cinfly.dadaxiaopu.service;

import com.cinfly.dadaxiaopu.dto.Result;
import com.cinfly.dadaxiaopu.entity.Follow;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
public interface IFollowService extends IService<Follow> {

    Result follow(Long followUserId, Boolean isFollow);

    Result followOrNot(Long followUserId);

    Result common(Long id);
}
