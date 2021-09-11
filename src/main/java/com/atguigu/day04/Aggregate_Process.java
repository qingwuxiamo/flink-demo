package com.atguigu.day04;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;

/**
 * 每个用户每5秒钟窗口的pv
 * 增量聚合函数和全窗口聚合函数结合使用
 * <p>
 * 增量：  节省内存        但获取不了状态
 * 全窗口：可以获得状态     但浪费内存
 *
 * @author zhoums
 * @version 1.0
 * @date 2021/9/10 19:48
 */
public class Aggregate_Process {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                //先分组
                .keyBy(t -> t.user)
                //再开窗
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //aggregate方法， 结合增量和全窗口
                .aggregate(new CountAgg(), new WindowResult())
                .print();

        env.execute();

    }

    /**
     * 增量负责计算
     */
    public static class CountAgg implements AggregateFunction<Event, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Event value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }

    /**
     * 输入的泛型是增量聚合函数的输出的类型
     * 全窗口获得状态
     */
    public static class WindowResult extends ProcessWindowFunction<Integer, String, String, TimeWindow> {
        /**
         * 在窗口关闭的时候，触发调用
         * 迭代器参数中只包含一个元素，就是增量聚合函数发送过来的聚合结果
         * @param key
         * @param context
         * @param elements
         * @param out
         * @throws Exception
         */
        @Override
        public void process(String key, Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
            long windowStart = context.window().getStart();
            long windowEnd = context.window().getEnd();
            long count = elements.iterator().next();
            out.collect("用户：" + key + " 在窗口" +
                    "" + new Timestamp(windowStart) + "~" + new Timestamp(windowEnd) + "" +
                    "中的pv次数是：" + count);
        }
    }

    // SourceFunction并行度只能为1
    // 自定义并行化版本的数据源，需要使用ParallelSourceFunction
    public static class ClickSource implements SourceFunction<Event> {
        private boolean running = true;
        private String[] userArr = {"Mary", "Bob", "Alice", "Liz"};
        private String[] urlArr = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
        private Random random = new Random();

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            while (running) {
                // collect方法，向下游发送数据
                ctx.collect(
                        new Event(
                                userArr[random.nextInt(userArr.length)],
                                urlArr[random.nextInt(urlArr.length)],
                                Calendar.getInstance().getTimeInMillis()
                        )
                );
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class Event {
        public String user;
        public String url;
        public Long timestamp;

        public Event() {
        }

        public Event(String user, String url, Long timestamp) {
            this.user = user;
            this.url = url;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "user='" + user + '\'' +
                    ", url='" + url + '\'' +
                    ", timestamp=" + new Timestamp(timestamp) +
                    '}';
        }
    }


}
