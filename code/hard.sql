-- 1. 同时在线人数问题
-- 现有各直播间的用户访问记录表（live_events）如下，表中每行数据表达的信息为，一个用户何时进入了一个直播间，又在何时离开了该直播间
-- 1.1 建表语句
drop table if exists live_events;
create table if not exists live_events
(
    user_id      int comment '用户id',
    live_id      int comment '直播id',
    in_datetime  string comment '进入直播间时间',
    out_datetime string comment '离开直播间时间'
)
    comment '直播间访问记录';

-- 1.2 数据装载
INSERT overwrite table live_events
VALUES (100, 1, '2021-12-01 19:00:00', '2021-12-01 19:28:00'),
       (100, 1, '2021-12-01 19:30:00', '2021-12-01 19:53:00'),
       (100, 2, '2021-12-01 21:01:00', '2021-12-01 22:00:00'),
       (101, 1, '2021-12-01 19:05:00', '2021-12-01 20:55:00'),
       (101, 2, '2021-12-01 21:05:00', '2021-12-01 21:58:00'),
       (102, 1, '2021-12-01 19:10:00', '2021-12-01 19:25:00'),
       (102, 2, '2021-12-01 19:55:00', '2021-12-01 21:00:00'),
       (102, 3, '2021-12-01 21:05:00', '2021-12-01 22:05:00'),
       (104, 1, '2021-12-01 19:00:00', '2021-12-01 20:59:00'),
       (104, 2, '2021-12-01 21:57:00', '2021-12-01 22:56:00'),
       (105, 2, '2021-12-01 19:10:00', '2021-12-01 19:18:00'),
       (106, 3, '2021-12-01 19:01:00', '2021-12-01 21:10:00');

-- 1.3 代码实现
-- 查询字段: live_id	max_user_count
select live_id,
       max(online_people) max_user_count
from (select live_id,
             sum(flag) over (partition by live_id order by this_time) online_people
      from (select live_id,
                   in_datetime this_time,
                   1           flag
            from live_events
            union all
            select live_id,
                   out_datetime this_time,
                   -1           flag
            from live_events) t1) t2
group by live_id;

-- 2. 会话划分问题
-- 现有页面浏览记录表（page_view_events）如下，表中有每个用户的每次页面访问记录
-- 2.1 建表语句
drop table if exists page_view_events;
create table if not exists page_view_events
(
    user_id        int comment '用户id',
    page_id        string comment '页面id',
    view_timestamp bigint comment '访问时间戳'
)
    comment '页面访问记录';

-- 2.2 数据装载
insert overwrite table page_view_events
values (100, 'home', 1659950435),
       (100, 'good_search', 1659950446),
       (100, 'good_list', 1659950457),
       (100, 'home', 1659950541),
       (100, 'good_detail', 1659950552),
       (100, 'cart', 1659950563),
       (101, 'home', 1659950435),
       (101, 'good_search', 1659950446),
       (101, 'good_list', 1659950457),
       (101, 'home', 1659950541),
       (101, 'good_detail', 1659950552),
       (101, 'cart', 1659950563),
       (102, 'home', 1659950435),
       (102, 'good_search', 1659950446),
       (102, 'good_list', 1659950457),
       (103, 'home', 1659950541),
       (103, 'good_detail', 1659950552),
       (103, 'cart', 1659950563);

-- 规定若同一用户的相邻两次访问记录时间间隔小于60s，则认为两次浏览记录属于同一会话。
-- 现有如下需求，为属于同一会话的访问记录增加一个相同的会话id字段
-- 2.3 代码实现
-- 查询字段: user_id	page_id	view_timestamp	session_id

select user_id,
       page_id,
       view_timestamp,
       concat(user_id, '-', sum(id_incre) over (partition by user_id order by view_timestamp)) session_id
from (select user_id,
             page_id,
             view_timestamp,
             if(view_timestamp - lag_view_timestamp >= 60, 1, 0) id_incre
      from (select user_id,
                   page_id,
                   view_timestamp,
                   lag(view_timestamp, 1, 0) over (partition by user_id order by view_timestamp) lag_view_timestamp
            from page_view_events) t1) t2

-- 3. 间断连续登录用户问题
-- 现有各用户的登录记录表（login_events）如下，表中每行数据表达的信息是一个用户何时登录了平台
-- user_id	login_datetime
-- 100	2021-12-01 19:00:00
-- 100	2021-12-01 19:30:00
-- 100	2021-12-02 21:01:00
-- 现要求统计各用户最长的连续登录天数，间断一天也算作连续，例如：一个用户在1,3,5,6登录，则视为连续6天登录
-- 3.1 建表语句
drop table if exists login_events;
create table if not exists login_events
(
    user_id        int comment '用户id',
    login_datetime string comment '登录时间'
)
    comment '直播间访问记录';

-- 3.2 数据装载
INSERT overwrite table login_events
VALUES (100, '2021-12-01 19:00:00'),
       (100, '2021-12-01 19:30:00'),
       (100, '2021-12-02 21:01:00'),
       (100, '2021-12-03 11:01:00'),
       (101, '2021-12-01 19:05:00'),
       (101, '2021-12-01 21:05:00'),
       (101, '2021-12-03 21:05:00'),
       (101, '2021-12-05 15:05:00'),
       (101, '2021-12-06 19:05:00'),
       (102, '2021-12-01 19:55:00'),
       (102, '2021-12-01 21:05:00'),
       (102, '2021-12-02 21:57:00'),
       (102, '2021-12-03 19:10:00'),
       (104, '2021-12-04 21:57:00'),
       (104, '2021-12-02 22:57:00'),
       (105, '2021-12-01 10:01:00');

-- 3.3 代码实现
-- 查询字段: user_id	max_day_count

-- select
--     user_id,
--     sum(`if`(diff <= 1,1,0)) as max_day_count
-- from (select user_id,
--              login_date,
--              datediff(login_continous, lag(login_continous, 1, login_continous)
--                                            over (partition by user_id order by login_continous)) as diff
--       from (select user_id,
--                    login_date,
--                    date_sub(login_date, rk) login_continous
--             from (select user_id,
--                          login_date,
--                          rank() over (partition by user_id order by login_date) rk
--                   from (select user_id,
--                                to_date(login_datetime) login_date
--                         from login_events
--                         group by user_id, to_date(login_datetime)) t1) t2) t3) t4
-- group by user_id;


select user_id,
       max(continuous_days) max_count_days
from (select user_id,
             datediff(max(login_date), min(login_date)) + 1 continuous_days
      from (select user_id,
                   login_date,
                   sum(`if`(datediff(login_date, lag_login_date) > 2, 1, 0))
                       over (partition by user_id order by login_date) flag
            from (select user_id,
                         login_date,
                         lag(login_date, 1, login_date) over (partition by user_id order by login_date) lag_login_date
                  from (select user_id,
                               to_date(login_datetime) login_date
                        from login_events
                        group by user_id, to_date(login_datetime)) t1) t2) t3
      group by user_id, flag) t4
group by user_id
order by user_id;

-- 4. 日期交叉问题
-- 现有各品牌优惠周期表（promotion_info）如下，其记录了每个品牌的每个优惠活动的周期，其中同一品牌的不同优惠活动的周期可能会有交叉
-- promotion_id	brand	start_date	end_date
-- 1	oppo	2021-06-05	2021-06-09
-- 2	oppo	2021-06-11	2021-06-21
-- 3	vivo	2021-06-05	2021-06-15

-- 现要求统计每个品牌的优惠总天数，若某个品牌在同一天有多个优惠活动，则只按一天计算
--4.1 建表语句
drop table if exists promotion_info;
create table promotion_info
(
    promotion_id string comment '优惠活动id',
    brand        string comment '优惠品牌',
    start_date   string comment '优惠活动开始日期',
    end_date     string comment '优惠活动结束日期'
) comment '各品牌活动周期表';

-- 4.2 数据装载
insert overwrite table promotion_info
values (1, 'oppo', '2021-06-05', '2021-06-09'),
       (2, 'oppo', '2021-06-11', '2021-06-21'),
       (3, 'vivo', '2021-06-05', '2021-06-15'),
       (4, 'vivo', '2021-06-09', '2021-06-21'),
       (5, 'redmi', '2021-06-05', '2021-06-21'),
       (6, 'redmi', '2021-06-09', '2021-06-15'),
       (7, 'redmi', '2021-06-17', '2021-06-26'),
       (8, 'huawei', '2021-06-05', '2021-06-26'),
       (9, 'huawei', '2021-06-09', '2021-06-15'),
       (10, 'huawei', '2021-06-17', '2021-06-21');

-- 4.3 代码实现
-- 查询字段: brand | promotion_day_count
select brand,
       sum(datediff(end_date, start_date) + 1) promotion_day_count
from (select brand,
             max_end_date,
             if(max_end_date is null or start_date > max_end_date, start_date, date_add(max_end_date, 1)) start_date,
             -- ，因为交叉日期不重复计算，所以当天的开始日期应该空最大结束日期的后一天开始计算
             end_date
      from (select brand,
                   start_date,
                   end_date,
                   max(end_date)
                       over (partition by brand order by start_date rows between unbounded preceding and 1 preceding) max_end_date
            -- 计算出到前一天为止的最大结束日期
            from promotion_info) t1) t2
where end_date > start_date
group by brand;