-- 1.建表

-- 创建学生表
DROP TABLE IF EXISTS student;
create table if not exists student_info
(
    stu_id   string COMMENT '学生id',
    stu_name string COMMENT '学生姓名',
    birthday string COMMENT '出生日期',
    sex      string COMMENT '性别'
)
    row format delimited fields terminated by ','
    stored as textfile;
-- 创建课程表
DROP TABLE IF EXISTS course;
create table if not exists course_info
(
    course_id   string COMMENT '课程id',
    course_name string COMMENT '课程名',
    tea_id      string COMMENT '任课老师id'
)
    row format delimited fields terminated by ','
    stored as textfile;

-- 创建老师表
DROP TABLE IF EXISTS teacher;
create table if not exists teacher_info
(
    tea_id   string COMMENT '老师id',
    tea_name string COMMENT '学生姓名'
)
    row format delimited fields terminated by ','
    stored as textfile;

-- 创建分数表
DROP TABLE IF EXISTS score;
create table if not exists score_info
(
    stu_id    string COMMENT '学生id',
    course_id string COMMENT '课程id',
    score     int COMMENT '成绩'
)
    row format delimited fields terminated by ','
    stored as textfile;

-- 数据导入
load data local inpath '/opt/datas/primarySQLTest/student_info.txt' into table student_info;
load data local inpath '/opt/datas/primarySQLTest/course_info.txt' into table course_info;
load data local inpath '/opt/datas/primarySQLTest/teacher_info.txt' into table teacher_info;
load data local inpath '/opt/datas/primarySQLTest/score_info.txt' into table score_info;

-- 验证导入情况
select *
from student_info
limit 5;
select *
from course_info
limit 5;
select *
from teacher_info
limit 5;
select *
from score_info
limit 5;

-- 2.简单查询

-- 查找特定条件
-- 2.1 查询姓名中带“冰”的学生名单
select *
from student_info
where stu_name like '%冰%';

-- 2.2 查询姓“王”老师的个数
select count(*) as num
from teacher_info
where tea_name like '王%';

-- 2.3 检索课程编号为“04”且分数小于60的学生的课程信息，结果按分数降序排列
select stu_id,
       course_id,
       score
from score_info
where course_id = '04'
  and score < 60
order by score desc;

-- 2.4 查询数学成绩不及格的学生和其对应的成绩，按照学号升序排序
select s1.stu_id stu_id,
       stu_name,
       score
from student_info s1
         join (select stu_id,
                      score
               from course_info c
                        join score_info s on c.course_id = s.course_id
               where c.course_name = '数学'
                 and s.score < 60) t
              on s1.stu_id = t.stu_id;

-- 3.汇总分析

-- 汇总分析
-- 3.1 查询编号为“02”的课程的总成绩
select sum(score) sum_score
from score_info
where course_id = '02';

-- 3.2 查询参加考试的学生个数
select count(distinct stu_id) stu_num
from score_info;

-- 分组
-- 3.3 查询各科成绩最高和最低的分，以如下的形式显示：课程号，最高分，最低分
select course_id,
       max(score) max_score,
       min(score) min_score
from score_info
group by course_id;

--3.4 查询每门课程有多少学生参加了考试
select course_id,
       count(distinct stu_id)
from score_info
group by course_id;

-- 3.5 查询男生、女生人数
select sum(if(sex = "男", 1, 0)) male_num,
       sum(if(sex = "女", 1, 0)) female_num
from student_info;

-- 分组结果的条件【having】
-- 3.6 查询平均成绩大于60分的学生的学号和平均成绩
select stu_id,
       avg(score) avg_score
from score_info
group by stu_id
having avg_score > 60;

-- 3.7 查询至少选修四门课程的学生学号
select stu_id
from score_info
group by stu_id
having count(course_id) >= 4;

-- 3.8 查询同姓（假设每个学生姓名的第一个字为姓）的学生名单并统计同姓人数大于2的姓
select t1.first_name,
       count(t1.first_name) name_num
from (select substr(stu_name, 0, 1) first_name
      from student_info) t1
group by t1.first_name
having name_num >= 2;

-- 3.9 查询每门课程的平均成绩，结果按平均成绩升序排序，平均成绩相同时，按课程号降序排列
select course_id,
       avg(score) avg_score
from score_info
group by course_id
order by avg_score asc, course_id desc;

-- 3.10 统计参加考试人数大于等于15的学科
select course_id,
       count(*) num
from score_info
group by course_id
having num >= 15;

-- 查询结果排序
-- 3.11 查询学生的总成绩并按照总成绩降序排序
select stu_id,
       sum(score) sum_score
from score_info
group by stu_id
order by sum_score desc;

-- 3.12 按照如下格式显示学生的语文、数学、英语三科成绩，没有成绩的输出为0，按照学生的有效平均成绩降序显示
select stu_id,
       sum(if(course_name = '语文', score, 0)) `语文`,
       sum(if(course_name = '数学', score, 0)) `数学`,
       sum(if(course_name = '英语', score, 0)) `英语`,
       count(ci.course_id)                     `有效课程数`,
       avg(score)                              `平均成绩`
from score_info si
         join course_info ci on si.course_id = ci.course_id
group by stu_id
order by `平均成绩` desc;

-- 3.13 查询一共参加三门课程且其中一门为语文课程的学生的id和姓名
select s1.stu_id,
       stu_name
from student_info s1
         join (select stu_id
               from (select stu_id,
                            course_id
                     from score_info
                     where stu_id in (select stu_id
                                      from score_info
                                      where course_id = '01')) t1
               group by stu_id
               having count(t1.course_id) = 3) t2
              on s1.stu_id = t2.stu_id;

-- 4.复杂查询
-- 子查询
-- 4.1 查询所有课程成绩均小于60分的学生的学号、姓名
select stu_id,
       stu_name
from student_info
where stu_id in
      (select stu_id
       from score_info
       group by stu_id
       having sum(`if`(score < 60, 0, 1)) = 0);

-- 4.2 查询没有学全所有课的学生的学号、姓名
-- 注意: 这里的"没学全"包括两种情况 ①学的课程数不足总课程数 ②压根一门课程都没学
-- 如果不使用left join，可能会漏掉情况②
select si.stu_id, stu_name
from student_info si
         left join score_info si
                   on si.stu_id = si.stu_id
group by si.stu_id, stu_name
having count(course_id) < (select count(course_id) from course_info);

-- 4.3 查询出只选修了三门课程的全部学生的学号和姓名
select stu_id, stu_name
from student_info
where stu_id in
      (select stu_id
       from score_info
       group by stu_id
       having count(course_id) = 3);


-- 5.多表查询

-- 表连结
-- 5.1 查询有两门以上的课程不及格的同学的学号及其平均成绩
select si.stu_id, avg(si.score) avg_score
from student_info si
         join score_info si on si.stu_id = si.stu_id
group by si.stu_id
having sum(`if`(si.score < 60, 1, 0)) >= 2;

-- 5.2 查询所有学生的学号、姓名、选课数、总成绩
select si.stu_id, stu_name, count(course_id) course_count, sum(score) sum_score
from student_info si
         left join score_info si on si.stu_id = si.stu_id
group by si.stu_id, stu_name;

--5.3 查询平均成绩大于85的所有学生的学号、姓名和平均成绩
select si.stu_id, stu_name, avg(score) avg_score
from student_info si
         left join score_info si on si.stu_id = si.stu_id
group by si.stu_id, stu_name
having avg_score > 85;

-- 5.4 查询学生的选课情况：学号，姓名，课程号，课程名称
select sc.stu_id, stu_name, ci.course_id, course_name
from score_info sc
         join course_info ci on sc.course_id = ci.course_id
         join student_info si on sc.stu_id = si.stu_id;

-- 5.5 查询出每门课程的及格人数和不及格人数

select ci.course_id,
       course_name,
       t.`及格人数`,
       t.`不及格人数`
from course_info ci
         join (select course_id,
                      sum(`if`(score >= 60, 1, 0)) as `及格人数`,
                      sum(`if`(score < 60, 1, 0))  as `不及格人数`
               from score_info
               group by course_id) t
              on t.course_id = ci.course_id;


-- 5.6 查询课程编号为03且课程成绩在80分以上的学生的学号和姓名及课程信息
select si.stu_id, stu_name, ci.course_id, course_name, score
from student_info si
         join
     (select stu_id, course_id, score
      from score_info
      where score > 80
        and course_id = '03') t
     on si.stu_id = t.stu_id
         join course_info ci on t.course_id = ci.course_id;

-- 多表连接
-- 5.7 课程编号为"01"且课程分数小于60，按分数降序排列的学生信息
select si.stu_id,
       stu_name,
       birthday,
       sex,
       score
from student_info si
         join
     (select stu_id, score
      from score_info
      where course_id = '01'
        and score < 60) t
     on si.stu_id = t.stu_id
order by score desc;

-- 5.8 查询所有课程成绩在70分以上的学生的姓名、课程名称和分数，按分数升序排列

select stu_name,
       course_name,
       score
from student_info si
         join
     (select stu_id
      from score_info
      group by stu_id
      having sum(`if`(score <= 70, 1, 0)) = 0) t
     on si.stu_id = t.stu_id
         join score_info si2 on t.stu_id = si2.stu_id
         join course_info ci on si2.course_id = ci.course_id
order by score;

-- 5.9 查询该学生不同课程的成绩相同的学生编号、课程编号、学生成绩
select si.stu_id, si.course_id, si.score
from score_info si
         join score_info si2
              on si.stu_id = si2.stu_id
where si.course_id != si2.course_id
  and si.score = si2.score;

-- 5.10 查询课程编号为“01”的课程比“02”的课程成绩高的所有学生的学号
select si.stu_id
from score_info si
         join score_info si2
              on si.stu_id = si2.stu_id
where si.course_id = '01' and si2.course_id = '02' and si.score > si2.score;

-- 5.11 查询学过编号为“01”的课程并且也学过编号为“02”的课程的学生的学号、姓名
select si.stu_id,
       stu_name
from (select stu_id
      from score_info
      where course_id = '01'
        and stu_id in
            (select stu_id
             from score_info
             where course_id = '02')) t
         join student_info si
              on si.stu_id = t.stu_id;

-- 方法2 通过having确保这两门课程都有成绩
select s.stu_id,stu_name
from student_info si
join score_info s on si.stu_id = s.stu_id
where course_id in ('01','02')
group by s.stu_id,stu_name
having count(distinct course_id) = 2;

-- 5.12 查询学过“李体音”老师所教的所有课的同学的学号、姓名
-- 注意一点: CTE的作用域仅限于其后面的查询部分，因此在HAVING子句中需要使用子查询来引用t1的结果
with t1 as (select count(*) course_count
            from course_info ci
                     join teacher_info ti on ci.tea_id = ti.tea_id
            where ti.tea_name = '李体音')
select si.stu_id, stu_name
from (select stu_id
      from score_info
      where course_id in
            (select course_id
             from course_info ci
                      join teacher_info ti on ci.tea_id = ti.tea_id
             where ti.tea_name = '李体音')
      group by stu_id
      having count(stu_id) = (select course_count from t1)) t2
         join student_info si on si.stu_id = t2.stu_id;

-- 5.13 查询学过“李体音”老师所讲授的任意一门课程的学生的学号、姓名
select si.stu_id, stu_name
from (select stu_id
      from score_info
      where course_id in
            (select course_id
             from course_info ci
                      join teacher_info ti on ci.tea_id = ti.tea_id
             where ti.tea_name = '李体音')
      group by stu_id) t2
         join student_info si on si.stu_id = t2.stu_id;

-- 另一种写法(子查询)
select stu_id, stu_name
from student_info
where stu_id in (select stu_id
                 from score_info
                 where course_id in (select course_id
                                     from course_info ci
                                              join teacher_info ti on ci.tea_id = ti.tea_id
                                     where ti.tea_name = '李体音')
                 group by stu_id);

--5.14 查询没学过"李体音"老师讲授的任一门课程的学生学号、姓名
select stu_id, stu_name
from student_info
where stu_id not in
      (select stu_id
       from score_info
       where course_id in
             (select course_id
              from course_info ci
                       join teacher_info ti on ci.tea_id = ti.tea_id
              where ti.tea_name = '李体音')
       group by stu_id);

-- 5.15 查询至少有一门课与学号为“001”的学生所学课程相同的学生的学号和姓名
select si.stu_id, stu_name
from student_info si
         join score_info sc on si.stu_id = sc.stu_id
where course_id in
      (select course_id
       from score_info
       where stu_id = '001')
  and si.stu_id != '001'
group by si.stu_id, stu_name;

-- 5.16 按平均成绩从高到低显示所有学生的所有课程的成绩以及平均成绩

select si.stu_id, stu_name, ci.course_id, course_name, score, avg_score
from student_info si
         join score_info sc on si.stu_id = sc.stu_id
         join course_info ci on sc.course_id = ci.course_id
         join (select stu_id, avg(score) avg_score
               from score_info
               group by stu_id) t
              on si.stu_id = t.stu_id
order by avg_score desc ;
