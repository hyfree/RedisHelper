using github.hyfree.RedisHelper;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Text.Json;

namespace github.hyfree.RedisHelper.Demo
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Test();

            //封装的写法
            //指定连接的库，默认是0
            RedisAccessorHelper redis = new RedisAccessorHelper();

            #region 测试String

            //存字符串
            string str = "苍";
            redis.StringSet("name", str);//设置StringSet(key, value)
            string str1 = redis.StringGet("name");//结果：苍
            redis.StringSet("name_two", str, TimeSpan.FromSeconds(10));//设置时间，10s后过期。
            Console.WriteLine("string Key：{0}，Value：{1}", "A", str1);

            //存对象（需要序列化成字符串存储）
            Demo demo = new Demo()
            {
                Name = "苍",
                Age = 18,
                Height = 1.83
            };
            redis.StringSet("model", demo);//序列化以后存入库中
            var model = redis.StringGet<Demo>("model");//反序列化后取值
            Console.WriteLine("model Key：{0}，Value：{1}", "model", model);

            //增量、减量（默认值同为1）
            double increment = 0;
            double decrement = 0;
            for (int i = 0; i < 3; i++)
            {
                increment = redis.StringIncrement("Increment", 2);//增量,每次+2
                Console.WriteLine("Increment Key：{0}，Value：{1}", "Increment", increment);
            }
            for (int i = 0; i < 3; i++)
            {
                decrement = redis.StringDecrement("Decrement");//减量，每次-1
                Console.WriteLine("Decrement Key：{0}，Value：{1}", "Decrement", decrement);
            }

            #endregion String

            #region List
            //Redis列表是简单的字符串列表，按照插入顺序排序。你可以添加一个元素到列表的头部（左边）或者尾部（右边）
            //一个列表最多可以包含 232 - 1 个元素 (4294967295, 每个列表超过40亿个元素)。
            for (int i = 0; i < 10; i++)
            {
                redis.ListRightPush("list", i);//模拟队（入队，先进先出，这里是从右进）
            }
            for (int i = 10; i < 20; i++)
            {
                redis.ListLeftPush("list", i);//模拟栈（入栈，先进后出，这里是从左进）
            }
            var length = redis.ListLength("list");//长度
            Console.WriteLine("长度 Key：{0}，Value：{1}", "list", length);

            var rightPop = redis.ListRightPop<string>("list");//模拟队（出队，这里是从右出）
            Console.WriteLine("模拟出队 Key：{0}，Value：{1}", "list", rightPop);

            var leftpop = redis.ListLeftPop<string>("list");//模拟栈（出栈，这里是从左出）
            Console.WriteLine("模拟出栈 Key：{0}，Value：{1}", "list", leftpop);

            var list = redis.ListRange<int>("list");
            Console.WriteLine("list Key：{0}，Value：{1}", "list", list);

            #endregion List

            #region Hash

            //Redis hash 是一个string类型的field和value的映射表，hash特别适合用于存储对象。
            //Redis 中每个 hash 可以存储 232 - 1 键值对（40多亿）。
            //Hash 的存储，给我的感觉类似于关系型数据库。以下面的例子为例，存储一个 user 对象（关系型数据库里的表名）， cang、shan、yun （关系型数据库里的数据的主键、唯一值），json（字段）
            redis.HashSet("user", "cang", demo);
            redis.HashSet("user", "shan", new Demo() { Name = "山", Age = 20, Height = 1.73 });
            redis.HashSet("user", "yun", new Demo() { Name = "云", Age = 30, Height = 1.80 });

            //获取Model
            demo = redis.HashGet<Demo>("user", "cang");
            //获取LIst
            IList<Demo> demolist = redis.HashGetList<Demo>("user");

            #endregion Hash

            #region 发布订阅

            //Redis 发布订阅(pub/sub)是一种消息通信模式，可以用于消息的传输，Redis的发布订阅机制包括三个部分，发布者，订阅者和Channel。适宜做在线聊天、消息推送等。
            //发布者和订阅者都是Redis客户端，Channel则为Redis服务器端，发布者将消息发送到某个的频道，订阅了这个频道的订阅者就能接收到这条消息，客户端可以订阅任意数量的频道
            redis.Subscribe("Channel1");//订阅频道 Channel1
            for (int i = 0; i < 10; i++)
            {
                redis.Publish("Channel1", "msg" + i);//向频道 Channel1 发送信息
                if (i == 2)
                {
                    redis.Unsubscribe("Channel1");//取消订阅
                }
            }

            #endregion 发布订阅

            #region 事务

            //事物开启后，会在调用 Execute 方法时把相应的命令操作封装成一个请求发送给 Redis 一起执行。
            string name = redis.StringGet("name");
            string age = redis.StringGet("age");

            //这里通过CreateTransaction函数（multi）来创建一个事物，调用其Execute函数（exec）提交事物。
            //其中的 "Condition.StringEqual("name", name)" 就相当于Redis命令中的watch name。
            var tran = redis.CreateTransaction();//创建事物
            tran.AddCondition(Condition.StringEqual("name", name));//乐观锁
            tran.StringSetAsync("name", "海");
            tran.StringSetAsync("age", 25);
            redis.StringSet("name", "Cang");//此时更改name值，提交事物的时候会失败。
            bool committed = tran.Execute();//提交事物，true成功，false回滚。
            //因为提交事物的过程中，name 值被修改，所以造成了回滚，所有给 name 赋值海，age 赋值25都失败了。

            #endregion 事务

            #region Batch

            //batch会把所需要执行的命令打包成一条请求发到Redis，然后一起等待返回结果。减少网络开销。
            var batch = redis.CreateBatch();

            //批量写
            Task t1 = batch.StringSetAsync("name", "羽");
            Task t2 = batch.StringSetAsync("age", 22);
            batch.Execute();
            Task.WaitAll(t1, t2);
            Console.WriteLine("Age:" + redis.StringGet("age"));
            Console.WriteLine("Name:" + redis.StringGet("name"));

            //批量写
            for (int i = 0; i < 100000; i++)
            {
                batch.StringSetAsync("age" + i, i);
            }
            batch.Execute();

            //批量读
            List<Task<RedisValue>> valueList = new List<Task<RedisValue>>();
            for (int i = 0; i < 10000; i++)
            {
                Task<RedisValue> tres = batch.StringGetAsync("age" + i);
                valueList.Add(tres);
            }
            batch.Execute();
            foreach (var redisValue in valueList)
            {
                string value = redisValue.Result;//取出对应的value值
            }

            #endregion

            #region Lock（分布式锁）

            //由于Redis是单线程模型，命令操作原子性，所以利用这个特性可以很容易的实现分布式锁。
            //lock_key表示的是redis数据库中该锁的名称，不可重复。 
            //token用来标识谁拥有该锁并用来释放锁。
            //TimeSpan表示该锁的有效时间。10秒后自动释放，避免死锁。
            var db = redis.GetDatabase();
            RedisValue token = Environment.MachineName;
            if (db.LockTake("lock_key", token, TimeSpan.FromSeconds(10)))
            {
                try
                {
                    //TODO:开始做你需要的事情
                    Thread.Sleep(5000);
                }
                finally
                {
                    db.LockRelease("lock_key", token);//释放锁
                }
            }

            #endregion Lock

            Console.ReadLine();
        }

        //未封装的写法
        public static void Test()
        {
            ConnectionMultiplexer _conn = ConnectionMultiplexer.Connect("127.0.0.1:6379,allowadmin=true");//初始化
            var database = _conn.GetDatabase(1);//指定连接库 1

            #region String

            database.StringSet("name", "苍");//设置StringSet(key, value)
            string str = database.StringGet("name");//结果：苍
            database.StringSet("name_two", str, TimeSpan.FromSeconds(10));//设置时间，10s后过期。

            //存取对象（对象需要序列化转成字符串，再存进库中）
            //创建对象
            Demo demo = new Demo()
            {
                Name = "苍",
                Age = 18,
                Height = 1.83
            };
        
            string demojson = JsonSerializer.Serialize(demo);//序列化
            database.StringSet("model", demojson);
            string model = database.StringGet("model");
            demo = JsonSerializer.Deserialize<Demo>(model);//反序列化

            //StringIncrement增量、StringDecrement减量（默认值同为1）
            double increment = 0;
            double decrement = 0;
            for (int i = 0; i < 3; i++)
            {
                increment = database.StringIncrement("Increment", 2);//增量,每次+2
                Console.WriteLine("Increment Key：{0}，Value：{1}", "Increment", increment);
            }
            for (int i = 0; i < 3; i++)
            {
                decrement = database.StringDecrement("Decrement");//减量，每次-1
                Console.WriteLine("Decrement Key：{0}，Value：{1}", "Decrement", decrement);
            }

            #endregion

            #region List

            //Redis列表是简单的字符串列表，按照插入顺序排序。你可以添加一个元素到列表的头部或者尾部
            //一个列表最多可以包含 232 - 1 个元素 (4294967295, 每个列表超过40亿个元素)。
            for (int i = 0; i < 10; i++)
            {
                database.ListRightPush("list", i);//入队，先进先出，这里是从底部插入数据
            }
            for (int i = 10; i < 20; i++)
            {
                database.ListLeftPush("list", i);//入栈，先进后出，这里是从顶部插入数据
            }
            var length = database.ListLength("list");//长度 20

            var rightPop = database.ListRightPop("list");//出队，这里是从底部拿出数据
            Console.WriteLine("模拟出队 Key：{0}，Value：{1}", "list", rightPop);

            var leftpop = database.ListLeftPop("list");//出栈，这里是从顶部拿出数据
            Console.WriteLine("模拟出栈 Key：{0}，Value：{1}", "list", leftpop);

            var list = database.ListRange("list");
            Console.WriteLine("list Key ：{0}，Value：{1}", "list", list);

            #endregion List

            #region Hash

            //Redis hash 是一个string类型的field和value的映射表，hash特别适合用于存储对象。
            //Redis 中每个 hash 可以存储 232 - 1 键值对（40多亿）。
            //Hash 的存储，给我的感觉类似于关系型数据库。以下面的例子为例，存储一个 user 对象（关系型数据库里的表名）， cang、shan、yun （关系型数据库里的数据的主键、唯一值），json（字段）
            string json = JsonSerializer.Serialize(demo);//序列化
            database.HashSet("user", "cang", json);
            database.HashSet("user", "shan", json);
            database.HashSet("user", "yun", json);

            //获取Model
            string hashcang = database.HashGet("user", "cang");
            demo = JsonSerializer.Deserialize<Demo>(hashcang);//反序列化

            //获取List
            RedisValue[] values = database.HashValues("user");//获取所有value
            IList<Demo> demolist = new List<Demo>();
            foreach (var item in values)
            {
                Demo hashmodel = JsonSerializer.Deserialize<Demo>(item);
                demolist.Add(hashmodel);
            }

            #endregion Hash

            #region 发布订阅

            //Redis 发布订阅(pub/sub)是一种消息通信模式，可以用于消息的传输，Redis的发布订阅机制包括三个部分，发布者，订阅者和Channel。适宜做在线聊天、消息推送等。
            //发布者和订阅者都是Redis客户端，Channel则为Redis服务器端，发布者将消息发送到某个的频道，订阅了这个频道的订阅者就能接收到这条消息，客户端可以订阅任意数量的频道

            ISubscriber sub = _conn.GetSubscriber();

            //订阅 Channel1 频道
            sub.Subscribe("Channel1", new Action<RedisChannel, RedisValue>((channel, message) =>
            {
                Console.WriteLine("Channel1" + " 订阅收到消息：" + message);
            }));

            for (int i = 0; i < 10; i++)
            {
                sub.Publish("Channel1", "msg" + i);//向频道 Channel1 发送信息
                if (i == 2)
                {
                    sub.Unsubscribe("Channel1");//取消订阅
                }
            }

            #endregion 发布订阅

            #region 事务

            //事物开启后，会在调用 Execute 方法时把相应的命令操作封装成一个请求发送给 Redis 一起执行。
            string name = database.StringGet("name");
            string age = database.StringGet("age");

            //这里通过CreateTransaction函数（multi）来创建一个事物，调用其Execute函数（exec）提交事物。
            //其中的 "Condition.StringEqual("name", name)" 就相当于Redis命令中的watch name。
            var tran = database.CreateTransaction();//创建事物
            tran.AddCondition(Condition.StringEqual("name", name));//乐观锁
            tran.StringSetAsync("name", "海");
            tran.StringSetAsync("age", 25);
            database.StringSet("name", "Cang");//此时更改name值，提交事物的时候会失败。
            bool committed = tran.Execute();//提交事物，true成功，false回滚。
            //因为提交事物的过程中，name 值被修改，所以造成了回滚，所有给 name 赋值海，age 赋值25都失败了。

            #endregion 事务

            #region Batch

            //batch会把所需要执行的命令打包成一条请求发到Redis，然后一起等待返回结果。减少网络开销。
            var batch = database.CreateBatch();

            //批量写
            Task t1 = batch.StringSetAsync("name", "羽");
            Task t2 = batch.StringSetAsync("age", 22);
            batch.Execute();
            Task.WaitAll(t1, t2);
            Console.WriteLine("Age:" + database.StringGet("age"));
            Console.WriteLine("Name:" + database.StringGet("name"));
            
            //批量写
            for (int i = 0; i < 100000; i++)
            {
                batch.StringSetAsync("age" + i, i);
            }
            batch.Execute();

            //批量读
            List<Task<RedisValue>> valueList = new List<Task<RedisValue>>();
            for (int i = 0; i < 10000; i++)
            {
                Task<RedisValue> tres = batch.StringGetAsync("age" + i);
                valueList.Add(tres);
            }
            batch.Execute();
            foreach (var redisValue in valueList)
            {
                string value = redisValue.Result;//取出对应的value值
            }

            #endregion

            #region Lock（分布式锁）

            //由于Redis是单线程模型，命令操作原子性，所以利用这个特性可以很容易的实现分布式锁。
            //lock_key表示的是redis数据库中该锁的名称，不可重复。 
            //token用来标识谁拥有该锁并用来释放锁。
            //TimeSpan表示该锁的有效时间。10秒后自动释放，避免死锁。
            RedisValue token = Environment.MachineName;
            if (database.LockTake("lock_key", token, TimeSpan.FromSeconds(10)))
            {
                try
                {
                    //TODO:开始做你需要的事情
                    Thread.Sleep(5000);
                }
                finally
                {
                    database.LockRelease("lock_key", token);//释放锁
                }
            }

            #endregion
        }
    }

    public class Demo
    {
        public string Name { get; set; }
        public int Age { get; set; }
        public double Height { get; set; }
    }
}