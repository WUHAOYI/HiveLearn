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
             count(distinct od.sku_id) over (partition by ci.category_id) as order_cnt
      from (select sku_id, sum(sku_num) as order_num
            from order_detail
            group by sku_id) od
               join sku_info si
                    on od.sku_id = si.sku_id
               join category_info ci
                    on si.category_id = ci.category_id) t1
where rn = 1;






