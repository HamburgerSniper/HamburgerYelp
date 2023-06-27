package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.FOLLOWS_KEY;

@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IUserService userService;

    /**
     * 关注用户
     *
     * @param followUserId
     * @param isFollow
     * @return
     */
    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        // 1.获取登录用户
        Long userId = UserHolder.getUser().getId();

        // 2.判断到底是关注还是取关
        if (isFollow) {
            // 3.关注。新增数据
            // 新增当前用户和关注用户的关系
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            // 保存到数据库
            boolean isSuccess = save(follow);
            if (isSuccess) {
                // 保存到redis里面去 sadd userId followUserId
                // 注意这个是我们自己关注的
                stringRedisTemplate.opsForSet().add(FOLLOWS_KEY + userId, followUserId.toString());

            }
        } else {
            // 4.不关注，删除 delete from tb_follow where userId = ? and follow_user_id = ?
            // mybatisplus的语法
            boolean isSuccess = remove(new QueryWrapper<Follow>().eq("user_id", userId).eq("follow_user_id", followUserId));

            // 成功的话
            if (isSuccess) {
                // 把取关的用户从redis中移除
                stringRedisTemplate.opsForSet().remove(FOLLOWS_KEY + userId, followUserId);
            }
        }

        return Result.ok();
    }

    /**
     * 判断是否关注
     * 就是显示出来
     *
     * @param followUsrId
     * @return
     */
    @Override
    public Result isFollow(Long followUsrId) {
        // 1.获取登录用户
        Long userId = UserHolder.getUser().getId();

        // 2.查询是否关注 select count(*) from tb_follow where user_id = userId and follow_user_id = followUserId
        Integer count = query().eq("user_id", userId).eq("follow_user_id", followUsrId).count();

        // 3.判断
        return Result.ok(count > 0);
    }

    /**
     * 查看共同关注
     *
     * @param followUserId
     * @return
     */
    @Override
    public Result followCommons(Long followUserId) {
        // 1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        // 1.1 自己的
        String key1 = FOLLOWS_KEY + userId;

        String key2 = FOLLOWS_KEY + followUserId;
        // 2.求交集
        Set<String> ids = stringRedisTemplate.opsForSet().intersect(key1, key2);

        // 2.1判断是否有交集
        if (ids == null || ids.isEmpty()) {
            // 就传一个空回去
            return Result.ok(Collections.emptyList());
        }
        // 3.解析出里面的id
        List<Long> idS = ids.stream().map(Long::valueOf).collect(Collectors.toList());

        // 4.查询用户(要转换为userDTO)
        List<UserDTO> userDTO = userService.listByIds(idS)
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());
        //
        return Result.ok(userDTO);
    }
}
