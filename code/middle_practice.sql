-- 1. 查询累积销量排名第二的商品
select sku_id
from (select sku_id,
             order_nums,
             dense_rank() over (order by order_nums desc) as rk
      from (select sku_id, sum(sku_id) as order_nums
            from order_detail
            group by sku_id) t1) t2
where t2.rk = 2;

-- 这里要看如何理解销量排名，如果遇到相同的排名，是否会跳过后续的排名数字
-- 如果不跳过的话，使用dense_rank即可，如果跳过的话，使用rank，就需要考虑不存在第二名的情况，如下

select sku_id
from (select sku_id, rk
      from (select sku_id,
                   order_nums,
                   rank() over (order by order_nums desc) as rk
            from (select sku_id, sum(sku_id) as order_nums
                  from order_detail
                  group by sku_id) t1) t2
      where t2.rk = 2) t3
         right join (select 1) t4;
-- 为保证，没有第二名的情况下，返回null

-- 2. 查询至少连续三天下单的用户
select user_id
from (select user_id,
             date_sub(create_date, row_number() over (partition by user_id order by create_date)) as target
      from order_info) t1
group by user_id, target
having count(target) >= 3;

-- 3. 查询各品类销售商品的种类数及销量最高的商品
select category_id,
       category_name,
       sku_id,
       name,
       order_num,
       order_cnt
from (select od.sku_id,
             name,
             ci.category_id,
             category_name,
             order_num,
             row_number() over (partition by ci.category_id order by order_num desc ) as rn,
             count(distinct od.sku_id) over (partition by ci.category_id)             as order_cnt
      from (select sku_id, sum(sku_num) as order_num
            from order_detail
            group by sku_id) od
               join sku_info si
                    on od.sku_id = si.sku_id
               join category_info ci
                    on si.category_id = ci.category_id) t1
where rn = 1;

-- 4. 查询用户的累计消费金额及VIP等级
-- VIP等级计算规则:
-- 设累积消费总额为X，
-- 若0=<X<10000,则vip等级为普通会员
-- 若10000<=X<30000,则vip等级为青铜会员
-- 若30000<=X<50000,则vip等级为白银会员
-- 若50000<=X<80000,则vip为黄金会员
-- 若80000<=X<100000,则vip等级为白金会员
-- 若X>=100000,则vip等级为钻石会员

select user_id,
       create_date,
       sum_so_far,
       case
           when sum_so_far < 10000 then '普通会员'
           when sum_so_far < 30000 then '青铜会员'
           when sum_so_far < 50000 then '白银会员'
           when sum_so_far < 80000 then '黄金会员'
           when sum_so_far < 100000 then '白金会员'
           else '钻石会员'
           end vip_level
from (select user_id,
             create_date,
             sum(total_amount_per_day) over (partition by user_id order by create_date) as sum_so_far
      from (select user_id,
                   create_date,
                   sum(total_amount) as total_amount_per_day
            from order_info
            group by user_id, create_date) t1 -- 从order_info查询各个用户当天的订单总金额
     ) t2;
-- 计算截至当日的订单总金额

-- 5. 查询首次下单后第二天连续下单的用户比率
select concat(round(sum(if(datediff(second_day, first_day) = 1, 1, 0)) / count(*) * 100, 1), '%') percentage
from (select user_id,
             min(create_date) first_day,
             max(create_date) second_day
      from (select user_id,
                   create_date,
                   rank() over (partition by user_id order by create_date) rk
            from (select user_id,
                         create_date
                  from order_info
                  group by user_id, create_date) t1 -- 查询出用户id和下单日期，并去重
           ) t2
      where rk <= 2
      group by user_id) t3;

-- 6. 每个商品销售首年的年份、销售数量和销售金额
select sku_id,
       year(create_date)    as first_year,
       sum(sku_num)         as order_num,
       sum(sku_num * price) as order_amount
from (select sku_id,
             create_date,
             sku_num,
             price,
             rank() over (partition by sku_id order by year(create_date)) rk
      from order_detail) t1
where rk = 1
group by sku_id, year(create_date);


-- 7.筛选去年总销量小于100的商品
-- 从订单明细表(order_detail)中筛选出去年总销量小于100的商品及其销量，假设今天的日期是2022-01-10，不考虑上架时间小于一个月的商品
-- 查询字段 商品id | 商品名称 | 销量
select t1.sku_id,
       name,
       order_amount
from (select sku_id,
             sum(sku_num) as order_amount
      from order_detail
      where year(create_date) = '2021'
        and sku_id not in
            (select sku_id
             from sku_info
             where datediff('2022-01-10', from_date) < 30)
      group by sku_id
      having sum(sku_num) < 100) t1
         join sku_info on t1.sku_id = sku_info.sku_id;

-- 8.查询每日新用户数
-- 从用户登录明细表（user_login_detail）中查询每天的新增用户数，若一个用户在某天登录了，且在这一天之前没登录过，则认为该用户为这一天的新增用户
-- 查询字段 login_date_first（日期）| user_count（新增用户数）

select login_date_first,
       count(user_id) user_count
from (select user_id,
             min(to_date(login_ts)) as login_date_first
      from user_login_detail
      group by user_id) t1
group by login_date_first
order by login_date_first;

-- 9. 统计每个商品的销量最高的日期
-- 如果有同一商品多日销量并列的情况，取其中的最小日期
-- 查询字段 sku_id（商品id）| create_date（销量最高的日期）| sum_num（销量）

select sku_id,
       create_date,
       sum_num
from (select sku_id,
             create_date,
             sum_num,
             rank() over (partition by sku_id order by sum_num desc,create_date) rk
      from (select sku_id,
                   create_date,
                   sum(sku_num) as sum_num
            from order_detail
            group by sku_id, create_date) t1) t2
where rk = 1;

-- 10. 查询销售件数高于品类平均数的商品
-- 查询字段 sku_id | name | sum_num | cate_avg_num

select sku_id,
       name,
       sum_num,
       cate_avg_num
from (select t1.sku_id,
             name,
             sum_num,
             avg(sum_num) over (partition by category_id) cate_avg_num
      from (select sku_id,
                   sum(sku_num) as sum_num
            from order_detail
            group by sku_id) t1
               join sku_info on t1.sku_id = sku_info.sku_id) t2
where sum_num > cate_avg_num;

-- 11. 用户注册、登录、下单综合统计
-- 从用户登录明细表（user_login_detail）和订单信息表（order_info）中查询每个用户的注册日期（首次登录日期）、总登录次数以及其在2021年的登录次数、订单数和订单总额
-- 查询字段 user_id(用户id)	register_date(注册日期)	total_login_count(累积登录次数)	login_count_2021(2021年登录次数)	order_count_2021(2021年下单次数)	order_amount_2021(2021年订单金额)

select t1.user_id,
       register_date,
       total_login_count,
       login_count_2021,
       order_count_2021,
       order_amount_2021
from (select user_id,
             min(to_date(login_ts)) as register_date,
             count(user_id)         as total_login_count
      from user_login_detail
      group by user_id) t1
         join (select user_id,
                      count(user_id) as login_count_2021
               from user_login_detail
               where year(to_date(login_ts)) = 2021
               group by user_id) t2
              on t1.user_id = t2.user_id
         join (select user_id,
                      count(user_id)    order_count_2021,
                      sum(total_amount) order_amount_2021
               from order_info
               where year(create_date) = 2021
               group by user_id) t3
              on t2.user_id = t3.user_id;

-- 12. 查询指定日期的全部商品价格
-- 从商品价格修改明细表（sku_price_modify_detail）中查询2021-10-01的全部商品的价格，假设所有商品初始价格默认都是99
-- 查询字段 sku_id（商品id）| price（商品价格）

-- 重点: nvl的用法

select t1.sku_id,
       nvl(new_price, 99) as price
from sku_info
         left join
     (select sku_id,
             new_price,
             rank() over (partition by sku_id order by change_date desc) rk
      from sku_price_modify_detail
      where change_date <= '2021-10-01') t1
     on t1.sku_id = sku_info.sku_id
where rk = 1;

-- 13. 即时订单比例
-- 订单配送中，如果期望配送日期和下单日期相同，称为即时订单，如果期望配送日期和下单日期不同，称为计划订单
-- 需求: 从配送信息表（delivery_info）中求出每个用户的首单（用户的第一个订单）中即时订单的比例，保留两位小数，以小数形式显示

-- 注意：求首单的时候不能用rank，而是要用row_number，避免重复
select round(sum(if(order_date = custom_date, 1, 0)) / count(user_id), 2) percentage
from (select user_id,
             order_date,
             custom_date,
             row_number() over (partition by user_id order by order_date) rn
      from delivery_info) t1
where rn = 1;

-- 14. 向用户推荐朋友收藏的商品
-- 向所有用户推荐其朋友收藏但是用户自己未收藏的商品
-- 查询字段 user_id（用户id） | sku_id（应向该用户推荐的商品id）


select distinct t1.user_id,
                friend_favor.sku_id
from (select user1_id user_id,
             user2_id friend_id
      from friendship_info
      union
      select user2_id,
             user1_id -- 筛选出所有(用户id,朋友id)
      from friendship_info) t1
         left join favor_info friend_favor
                   on t1.friend_id = friend_favor.user_id --以friend_id为连接字段，筛选出朋友收藏的商品
         left join favor_info user_favor
                   on t1.user_id = user_favor.user_id
                       and friend_favor.sku_id = user_favor.sku_id --以user_id | sku_id 为连接字段，从朋友收藏的商品数据中筛选出用户也收藏的商品
where user_favor.user_id is null;
-- 如果user_id为null，则说明当前商品只被朋友收藏，但用户未收藏

-- 15. 查询所有用户的连续登录两天及以上的日期区间
-- 从登录明细表（user_login_detail）中查询出，所有用户的连续登录两天及以上的日期区间，以登录时间（login_ts）为准
-- 查询字段：user_id(用户id) | start_date(开始日期) | end_date(结束日期)
select user_id,
       max(login_date) end_date,
       min(login_date) start_date
from (select user_id,
             date_sub(login_date, rn) sub,
             login_date
      from (select user_id,
                   to_date(login_ts)                                                   login_date,
                   row_number() over (partition by user_id order by to_date(login_ts)) rn
            from user_login_detail) t1) t2
group by user_id, sub
having count(user_id) >= 2;

-- 16. 男性和女性每日的购物总金额统计
-- 从订单信息表（order_info）和用户信息表（user_info）中，分别统计每天男性和女性用户的订单总金额，如果当天男性或者女性没有购物，则统计结果为0
-- 查询字段: create_date（日期） |　total_amount_male（男性用户总金额）｜total_amount_female（女性用户总金额）

select create_date,
       sum(`if`(gender = '男', total_amount, 0)) total_amount_male,
       sum(`if`(gender = '女', total_amount, 0)) total_amount_female
from order_info oi
         join user_info ui
              on oi.user_id = ui.user_id
group by create_date;

-- 17. 订单金额趋势分析
-- 查询截止每天的最近3天内的订单金额总和以及订单金额日平均值，保留两位小数，四舍五入
-- 查询字段: create_date | total_3d | avg_ad
select create_date,
       round(sum(total_amount_per_day) over (order by create_date rows between 2 preceding and current row),
             2)                                                                                                 total_3d,
       round(avg(total_amount_per_day) over (order by create_date rows between 2 preceding and current row), 2) avg_ad
from (select create_date,
             sum(total_amount) total_amount_per_day
      from order_info
      group by create_date) t1;

-- 18. 购买过商品1和商品2但是没有购买商品3的顾客
-- 重点考察hive的行转列

select user_id
from (select user_id,
             collect_set(sku_id) skus
      from order_detail
               join order_info on order_info.order_id = order_detail.order_id
      group by user_id) t1
where array_contains(skus, '1')
  and array_contains(skus, '2')
  and !array_contains(skus, '3');

-- 19. 统计每日商品1和商品2销量的差值
select create_date,
       sum(if(sku_id = '1', sku_num, 0)) - sum(if(sku_id = '2', sku_num, 0)) as diff
from order_detail
where sku_id in ('1', '2')
group by create_date
order by create_date;

-- 20. 查询出每个用户的最近三笔订单
-- 查询字段: user_id | order_id | create_date
select user_id,
       order_id,
       create_date
from (select user_id,
             order_id,
             create_date,
             row_number() over (partition by user_id order by create_date desc ) rn
      from order_info) t1
where rn <= 3;

-- 21. 查询每个用户登录日期的最大空档期
-- 从登录明细表（user_login_detail）中查询每个用户两个登录日期（以login_ts为准）之间的最大的空档期。统计最大空档期时，用户最后一次登录至今的空档也要考虑在内，假设今天为2021-10-10
-- 查询字段: user_id | max_diff（最大空档期）

select user_id,
       max(diff) max_diff
from (select user_id,
             datediff(next_date, login_date) diff
      from (select user_id,
                   login_date,
                   lead(login_date, 1, '2021-10-10') over (partition by user_id order by login_date) next_date
            from (select user_id,
                         to_date(login_ts) login_date
                  from user_login_detail
                  group by user_id, to_date(login_ts)) t1) t2) t3
group by user_id
order by user_id;

-- 22. 查询相同时刻多地登陆的用户
select distinct user_id
from (select user_id,
             if(pre_logout_ts is null, 1, if(pre_logout_ts < login_ts, 1, 0)) if_rep -- pre_logout_ts为null说明之前没有登录过
      from (select user_id,
                   login_ts,
                   logout_ts,
                   max(logout_ts)
                       over (partition by user_id order by login_ts rows between unbounded preceding and 1 preceding) pre_logout_ts
            from user_login_detail) t1) t2
where if_rep = 0;
-- 由于一个ip不可能出现logout_ts大于下一个login_ts的情况,所以一旦出现这种情况，就可以判定为异地登陆

-- **23. 销售额完成任务指标的商品
-- 商家要求每个商品每个月需要售卖出一定的销售总额 假设1号商品销售总额大于21000，2号商品销售总额大于10000，其余商品没有要求
-- 从订单详情表中（order_detail）查询连续两个月销售总额大于等于任务总额的商品

select sku_id
from (select sku_id,
             add_months(first_day, -row_number() over (partition by sku_id order by first_day)) month_continous
      from (select sku_id,
                   concat(date_format(create_date, 'yyyy-MM'), '-01') first_day,
                   sum(sku_num * price)                               sku_amount
            from order_detail
            where sku_id in ('1', '2')
            group by sku_id, concat(date_format(create_date, 'yyyy-MM'), '-01')
            having (sku_id = '1' and sku_amount > 21000)
                or (sku_id = '2' and sku_amount > 23000)) t1) t2
group by sku_id
having count(month_continous) >= 2;

-- 24. 根据商品销售情况进行商品分类
-- 从订单详情表中（order_detail）对销售件数对商品进行分类，0-5000为冷门商品，5001-19999位一般商品，20000往上为热门商品，并求出不同类别商品的数量
-- 查询字段: Category（类型）| Cn（数量）

select Category,
       count(sku_id) Cn
from (select case
                 when sum_num <= 5000 then '冷门商品'
                 when sum_num <= 19999 then '一般商品'
                 else '热门商品'
                 end Category,
             sku_id
      from (select sku_id,
                   sum(sku_num) as sum_num
            from order_detail
            group by sku_id) t1) t2
group by Category;

-- 25. 各品类销量前三的所有商品
-- 从订单详情表中（order_detail）和商品（sku_info）中查询各个品类销售数量前三的商品。如果该品类小于三个商品，则输出所有的商品销量

select sku_id,
       category_id
from (select sku_id,
             category_id,
             row_number() over (partition by category_id order by sum_num desc) rn
      from (select sku_info.sku_id,
                   category_id,
                   sum(sku_num) sum_num
            from order_detail
                     join sku_info on sku_info.sku_id = order_detail.sku_id
            group by sku_info.sku_id, category_id) t1) t2
where rn <= 3;

-- **26. 各品类中商品价格的中位数
-- 如果是偶数，则输出中间两个值的平均值;如果是奇数，则输出中间数即可
-- 查询字段: Category_id（品类id） | Medprice（中位数）

select category_id,
       round(sum(`if`(rn = sku_num / 2 or rn = sku_num / 2 + 1, price, 0)) / 2, 1) Medprice
from (select sku_id,
             category_id,
             price,
             count(*) over (partition by category_id)                    sku_num,
             count(*) over (partition by category_id) % 2                flag,
             row_number() over (partition by category_id order by price) rn
      from sku_info) t1
where flag = 0
group by category_id
union
select category_id,
       round(sum(`if`(rn = ceil(sku_num / 2), price, 0)) / 2, 1) Medprice
from (select sku_id,
             category_id,
             price,
             count(*) over (partition by category_id)                    sku_num,
             count(*) over (partition by category_id) % 2                flag,
             row_number() over (partition by category_id order by price) rn
      from sku_info) t1
where flag = 1
group by category_id;


-- 27. 找出销售额连续3天超过100的商品

select distinct sku_id
from (select sku_id,
             count(*) over (partition by sku_id,continous_flag) continous_count
      from (select sku_id,
                   date_sub(create_date, row_number() over (partition by sku_id order by create_date)) continous_flag
            from (select sku_id,
                         create_date,
                         sum(sku_num * price) sum_num
                  from order_detail
                  group by sku_id, create_date
                  having sum_num > 100) t1) t2) t3
where continous_count >= 3;

-- **28. 查询有新注册用户的当天的新用户数量、新用户的第一天留存率
-- (首次登录算作当天新增，第二天也登录了算作一日留存)
-- 查询字段: first_login（注册时间） | Register（新增用户数） | Retention（留存率）

select first_login,
       registe_user                  Register,
       retention_user / registe_user Retention
from (select first_login,
             count(t1.user_id)  registe_user,
             count(uld.user_id) retention_user
      from (select user_id,
                   min(to_date(login_ts)) first_login
            from user_login_detail
            group by user_id) t1
               left join user_login_detail uld
                         on t1.user_id = uld.user_id
                             and datediff(to_date(uld.login_ts), first_login) = 1
      group by first_login) t2;


-- 29. 求出商品连续售卖的时间区间
-- 查询字段: Sku_id（商品id） | Start_date（起始时间） | End_date（结束时间）

select distinct sku_id,
                first_value(create_date)
                            over (partition by sku_id, continous_flag order by create_date rows between unbounded preceding and unbounded following) Start_date,
                last_value(create_date)
                           over (partition by sku_id, continous_flag order by create_date rows between unbounded preceding and unbounded following)  End_date
from (select sku_id,
             create_date,
             date_sub(create_date, dense_rank() over (partition by sku_id order by create_date)) continous_flag
      from order_detail
      group by sku_id, create_date) t1
order by sku_id;

-- 需要注意:
-- 1.之所以要在t1中使用group by sku_id, create_date，是为了去重
-- 2.在选取first_value和last_value的时候，要注意窗口范围的限定: rows between unbounded preceding and unbounded following

-- 30. 用户每天的登录次数及交易次数统计
-- 查询字段: user_id | login_date | login_count | order_count

select
    t1.user_id,
    login_date,
    max(login_count) login_copunt,
    count(oi.user_id) order_count
from (select user_id,
             to_date(login_ts) login_date,
             count(*)          login_count
      from user_login_detail
      group by user_id, to_date(login_ts)) t1
left join order_info oi on t1.user_id = oi.user_id
and t1.login_date = oi.create_date
group by t1.user_id,login_date
order by t1.user_id;

-- 31. 按年度列出每个商品销售总额
-- 查询字段: Sku_id（商品id） | Year_date（年份） | Sku_sum（销售总额）
select
    sku_id,
    year(create_date) year_date,
    sum(price * sku_num) sku_sum
from order_detail
group by sku_id, year(create_date);

-- 32. 查询某周内每件商品每天销售情况(销售数量)
-- 从订单详情表（order_detail）中查询2021年9月27号-2021年10月3号这一周所有商品每天销售情况
-- 查询字段: Sku_id（商品id）	Monday	Tuesday	Wednesday	Thursday	Friday	Saturday	Sunday
select
    sku_id,
    sum(`if`(`dayofweek`(create_date) = 1,sku_num,0)) Sunday,
    sum(`if`(`dayofweek`(create_date) = 2,sku_num,0)) Monday,
    sum(`if`(`dayofweek`(create_date) = 3,sku_num,0)) Tuesday,
    sum(`if`(`dayofweek`(create_date) = 4,sku_num,0)) Wednesday,
    sum(`if`(`dayofweek`(create_date) = 5,sku_num,0)) Thursday,
    sum(`if`(`dayofweek`(create_date) = 6,sku_num,0)) Friday,
    sum(`if`(`dayofweek`(create_date) = 7,sku_num,0)) Saturday
from order_detail
where create_date >= '2021-09-27' and create_date <= '2021-10-03'
group by sku_id;


