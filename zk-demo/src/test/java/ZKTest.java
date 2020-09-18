import lombok.SneakyThrows;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
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
    public void testCRUD() throws KeeperException, InterruptedException {
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

    @Test
    public void testCloudConfig() throws InterruptedException {
        MyWatcher watcher = new MyWatcher(zooKeeper);
        zooKeeper.exists("/conf", watcher, watcher, "ABC");
        watcher.aWait();
    }

    class MyWatcher implements Watcher, AsyncCallback.StatCallback {
        private ZooKeeper zooKeeper;
        private CountDownLatch countDownLatch = new CountDownLatch(1);

        public MyWatcher(ZooKeeper zooKeeper) {
            this.zooKeeper = zooKeeper;
        }

        @SneakyThrows
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            byte[] data = zooKeeper.getData(path, false, stat);
            System.out.println(new String(data));
        }

        public void aWait() throws InterruptedException {
            countDownLatch.await();
        }

        @SneakyThrows
        @Override
        public void process(WatchedEvent event) {
            countDownLatch.countDown();
            String path = event.getPath();
            switch (event.getType()) {
                case None:
                    break;
                case NodeCreated:
                    System.out.println("data create:" + new String(zooKeeper.getData(path, false, new Stat())));
                    break;
                case NodeDeleted:
                    System.out.println("data delete:" + path);
                    break;
                case NodeDataChanged:
                    System.out.println("data change:" + new String(zooKeeper.getData(path, false, new Stat())));
                    break;
                case NodeChildrenChanged:
                    break;
            }
        }
    }

}
