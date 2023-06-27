package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.IFollowService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

@RestController
@RequestMapping("/follow")
public class FollowController {
    @Resource
    private IFollowService followService;


    /**
     * 关注用户
     *
     * @param followUserId
     * @param isFollow
     * @return
     */
    @PutMapping("/{id}/{isFollow}")
    public Result follow(@PathVariable("id") Long followUserId, @PathVariable("isFollow") Boolean isFollow) {
        return followService.follow(followUserId, isFollow);
    }

    /**
     * 查询是否自己是否关注
     *
     * @param followUsrId
     * @return
     */
    @GetMapping("/or/not/{id}")
    public Result isFollow(@PathVariable("id") Long followUsrId) {
        return followService.isFollow(followUsrId);
    }

    @GetMapping("/common/{id}")
    public Result followCommons(@PathVariable("id") Long followUserId) {

        return followService.followCommons(followUserId);
    }
}
