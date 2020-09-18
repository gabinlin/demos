import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.assertj.core.util.Maps;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class ZKTest {

    private ZooKeeper zooKeeper;

    @Before
    public void before() throws IOException {
        zooKeeper = new ZooKeeper("127.0.0.1:2181", 1000,
                watchedEvent -> System.out.println(watchedEvent.toString()));
    }

    @Test
    public void test() throws KeeperException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        // 增
        String path = "/gabin";
        String value = zooKeeper.create(path, "hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(value);
        // 改
        Map<String, Object> content = Maps.newHashMap("test", " true");
        zooKeeper.setData(path, "hello world".getBytes(), 0,
                (rc, p, ctx, stat) -> System.out.println(stat.toString()), content);
        // 查
        zooKeeper.getData(path, true, (rc, path1, ctx, data, stat) -> {
            System.out.println(data);
            // 删
            try {
                zooKeeper.delete(path, stat.getVersion());
                latch.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }, content);
        latch.await();
    }

}
