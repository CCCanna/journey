# journey *(Updated from journal)*
掌上武大数据分析报告
(Version 1.0 beta)

Code version 1.2 beta.

Compared with its former version, it has less bugs and unreasonable designs, which may be deserted or improved in coming versions.

Every step has data output in sql and csv formats, please use them on your demand. Metadata can be found in shimo document.

*When placed data in proper path, run python task.py to process the data.*

###### Warning: Never run the last 3 lines of code in task.py, it's the most unreasonable design in this code pack.

数据处理的每一步都有sql和csv两种格式的数据，请根据需要选择。原始数据可以在自强的石墨文档里面找到。

*(在某个风雨交加的夜晚，她看了看产品文档，安详离开了人世*

以下为已经实现与未被实现的功能列表：

- ## 常规数据指标
    1. #### 用户规模和质量✔
        1. 活跃用户指标✔
        2. 新增用户指标✗ *(喵喵喵？*
        3. 用户构成指标✗
            - 连续活跃N周用户✗
            - 忠诚用户✗
            - 连续活跃用户✗
            - 近期流失用户✗
        4. 用户留存率指标✗
        5. 每个用户总活跃天数指标✔
    2. #### 参与度分析✔ *后来发现设计问题把这个给整崩了*
        1. 启动次数指标✔
        2. 使用时长✗ *(时长这种东西不好算欸*
        3. 访问页面✗
        4. 使用时间间隔✗
    3. #### 渠道分析✗ *(臣妾做不到啊！*
    4. #### 功能分析✗ *(暂缓，目前不分析*
        1. 功能活跃指标✗
        2. 页面访问路径分析✗
        3. 转化率✗
    5. #### 用户属性和画像分析✗ *(某人偷懒决定将这个丢给后面的人去处理*
        1. 用户属性分析✗
        2. 用户画像分析✗

*只要可爱即使代码写的烂也是可以的喵？😉*

*嘤嘤嘤产品姐姐你看人家这么可爱那些没有处理的数据就算了吧😊*

[GitHub链接]https://github.com/CCCanna/journey

*亲亲这边建议看GitHub上的MarkDown效果更佳喔*

****

##### 自强Studio数据分析组荣誉出品
——By *Anna* 2019-09-01
