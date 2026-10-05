package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.IFollowService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/follow")
public class FollowController {

    @Resource
    private IFollowService followService;
    @PutMapping("/{id}/{isFollow}")
    public Result follow(@PathVariable("id") Long followUserId, @PathVariable("isFollow") Boolean isFollow) {
        return followService.follow(followUserId, isFollow);
    }
    @GetMapping("/or/not/{id}")
    public Result followOrNot(@PathVariable("id") Long followUserId) {
        return followService.followOrNot(followUserId);
    }

    @GetMapping("/common/{id}")
    public Result common(@PathVariable("id") Long id) {
        return followService.common(id);
    }

}
