package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IBlogService blogService;

    @Resource
    private IFollowService followService;


    /**
     * 分页查询
     *
     * @param current
     * @return
     */
    @Override
    public Result queryHotBlog(Integer current) {
        Page<Blog> page = blogService.query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            Long userId = blog.getUserId();
            User user = userService.getById(userId);
            blog.setName(user.getNickName());
            blog.setIcon(user.getIcon());
//            这个就是显示你是否点赞在主页上面
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }


    /**
     * 查询博客的笔记
     *
     * @param id
     * @return
     */
    @Override
    public Result queryById(Long id) {

        // 1.查询blog
        Blog blog = getById(id);

        if (blog == null) {
            return Result.fail("博客不存在");
        }
        // 2. 查询blog相关的用户
        Long userId = blog.getUserId();

        // 2.1 查询到博客相关的信息
        User user = userService.getById(userId);

        // blog就是返回给页面的
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());

        // 3.查询blog是否点赞
        isBlogLiked(blog);
        return Result.ok(blog);

    }

    // 这个就相当于界面显示你是否点赞就是红星
    private void isBlogLiked(Blog blog) {
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            // 因为用户可能未登录 无序查询是否点赞避免空指针
            return;
        }
        // 1.获取登录用户
        Long userId = UserHolder.getUser().getId();

        // 2.判断当前用户是否点赞
        Double score = stringRedisTemplate.opsForZSet().score(BLOG_LIKED_KEY + blog.getId(), userId.toString());

        // 这个islike就是表示你是否点赞了
        // 只要有时间戳表示点赞了
        blog.setIsLike(score != null);

    }


    /**
     * 点赞功能
     *
     * @param id
     * @return
     */
    @Override
    public Result likeBlog(Long id) {
        // 1.获取登录用户
        Long userId = UserHolder.getUser().getId();

        // 2.判断当前用户是否点赞
        // 2.1 去redis集合里面做判断
        // 这个key 就是我们的笔记的id
//        String key = "blog:like:" + id; // 这个id就是笔记的id
        // 查这个用户在这里面的时间
        Double score = stringRedisTemplate.opsForZSet().score(BLOG_LIKED_KEY + id, userId.toString());


        // 因为是包装类 所以可能为空 所以使用另外一种方法(现在改成时间戳了)
//        if (BooleanUtil.isFalse(isMember)) {
        if (score == null) {
            // 3.如果未点赞 可以点赞
            // 3.1数据库点赞数 +1
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            // 如果跟新成功
            if (isSuccess) {
                // 3.2将用户的信息保存到redis集合(由于现在是要看排序的所以我们要往sortset里面放)
//                stringRedisTemplate.opsForSet().add(BLOG_LIKED_KEY + id, userId.toString());
                // 第三位参数 是分数  所以我们以时间为分数就可以排序
                // 相当于 ZADD K1 VALUE SCORE
                stringRedisTemplate.opsForZSet().add(BLOG_LIKED_KEY + id, userId.toString(), System.currentTimeMillis());
            }

        } else {
            // 4.如果已经点赞
            //4.1 数据库点赞数 -1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            if (isSuccess) {
                // 4.2把这个用户从redis中移除
                stringRedisTemplate.opsForZSet().remove(BLOG_LIKED_KEY + id, userId.toString());
            }

        }

        return Result.ok();
    }

    /**
     * 去点赞排名的前5人
     *
     * @param id
     * @return
     */
    @Override
    public Result queryBlogLikes(Long id) {
        // 1.查询top5的用户点赞 zrange key 0 4(这个得到的是我们的时间戳)
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(BLOG_LIKED_KEY + id, 0, 4);

        // 注意这里要判断一下 因为 我们可能没有人点赞
        if (top5 == null || top5.isEmpty()) {
            return Result.ok();
        }

        // 2.解析出其中的用户id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",", ids); // 以我们的逗号分隔开每个id然后拼成字符串

        // 3.根据用户id来查询用户(注意我们是要点赞时间最早的在前面 但是我们获取的sql是in那么他就会自动根据用户id来排序)
        // 肯定是不行的所以我们要自己创建sql语句
//        List<User> users = userService.listByIds(ids);
        // 要添加 oreder by feile() 里面是你自己的排序顺序
        List<User> users = userService.query().in("id", ids).last("ORDER BY FIELD(id,  " + idStr + ")").list();
//        .list(); 转为我们的list集合

        // 4.注意我们要返回userDTO
        List<UserDTO> userDto = users.stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());

        // 4.返回
        return Result.ok(userDto);
    }

    /**
     * 查询这个用户发布了哪些笔记
     *
     * @param current
     * @param id
     * @return
     */
    @Override
    public Result queryBlogByUserId(Integer current, Long id) {
        // 根据用户查询
        Page<Blog> page = blogService.query()
                .eq("user_id", id).page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();

        return Result.ok(records);

    }

    /**
     * 保存自己写的探店笔记
     *
     * @param blog
     * @return
     */
    @Override
    public Result saveBlog(Blog blog) {
        // 1.获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 2.保存探店博文
        boolean isSuccess = blogService.save(blog);

        if (!isSuccess) {
            // 3.1新增笔记失败就不发送
            return Result.fail("新增笔记失败");
        }
        // 3.查询笔记作者的所有粉丝(写博客用户就是当前的登录用户)
        // 去粉丝关系数据库里面进去找
        // userId等于我们粉丝的id  follow_user_id等于我们的关注的那个人
        // 那么 我的粉丝就是 follow_user_id = userId 的那些人就是我的粉丝
        // select * from tb_follow where follow_user_id = userId
        List<Follow> fans = followService.query().eq("follow_user_id", user.getId()).list();

        // 4.推送笔记id给所有粉丝
        for (Follow fan : fans) {
            // 4.1获取粉丝的id
            Long userId = fan.getUserId();
            //4.2 进行推送(每一个粉丝的收件箱都是sortset)
            String key = FEED_KEY + userId;
            // 那么他的值就是我们关注的人发送的消息(value 是我们笔记的id)
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());


        }


        // 5.返回id
        return Result.ok(blog.getId());
    }

    /**
     * 粉丝接受关注者发布的探店笔记
     *
     * @param max
     * @param offset
     * @return
     */
    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        // 2.查询收件箱 ZREVRANGEBYSCORE key max Min LIMIT offset count
        String key = FEED_KEY + userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);

        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }

        // 3.解析数据 blogId score(时间戳)、 offset(跟我上次查询最小值元素)
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int os = 1; // 表示offset
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            // 4.1获取id
            String idStr = typedTuple.getValue();

            // 4.2添加到我们的集合里面去
            ids.add(Long.valueOf(idStr));

            // 4.3获取分数 也就是我们的时间戳 这个minTime最后一次肯定变成了我们的最小值
            long time = typedTuple.getScore().longValue(); // 得到long类型的

            // 4.4 offset 就是等于最小值的分数的个数
            if (time == minTime) {
                os++;
            } else {
                minTime = time;
                os = 1;
            }

        }
        String idStr = StrUtil.join(",", ids);
        // 4.根据id 封装并且返回(我们的ids是有序的 如果使用mybatisplus来实现的话就不是按照ids的顺序了)
        List<Blog> blogs = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();

        // 4.1我们要知道这个别人的笔记有哪些人给你点过赞  然后我们是否点过赞
        for (Blog blog : blogs) {
            // 4.2 查询blog相关的用户
            // 2. 查询blog相关的用户
            Long userid = blog.getUserId();

            // 2.1 查询到博客相关的信息
            User user = userService.getById(userid);

            // blog就是返回给页面的
            blog.setName(user.getNickName());
            blog.setIcon(user.getIcon());

            // 3.查询blog是否点赞
            isBlogLiked(blog);
        }

        // 5.封装并且返回
        ScrollResult r = new ScrollResult();
        r.setList(blogs);
        r.setOffset(os);
        r.setMinTime(minTime);
        return Result.ok(r);
    }
}
