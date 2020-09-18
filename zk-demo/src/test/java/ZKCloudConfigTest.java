import lombok.SneakyThrows;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZKCloudConfigTest {

    private ZooKeeper zooKeeper;

    @Before
    public void before() throws IOException {
        zooKeeper = new ZooKeeper("127.0.0.1:2181", 1000,
                watchedEvent -> System.out.println(watchedEvent.toString()));
    }

    @Test
    public void testCloudConfig() {
        MyWatcher watcher = new MyWatcher(zooKeeper);
        watcher.aWait();

        while (true) {

            if (watcher.getConf().equals("")) {
                System.out.println("conf diu le ......");
                watcher.aWait();
            } else {
                System.out.println(watcher.getConf());

            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    class MyWatcher implements Watcher, AsyncCallback.StatCallback, AsyncCallback.DataCallback {
        private ZooKeeper zooKeeper;
        private String conf;
        private CountDownLatch countDownLatch = new CountDownLatch(1);

        public MyWatcher(ZooKeeper zooKeeper) {
            this.zooKeeper = zooKeeper;
        }

        public String getConf() {
            return conf;
        }

        public void setConf(String conf) {
            this.conf = conf;
        }

        @SneakyThrows
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            if (stat != null) {
                // 2、监听节点回调后再次设置一个获取数据的回调
                zooKeeper.getData("/conf", this, this, "ABC");
            }
        }

        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            if (data != null) {
                // 3、获取数据回调成功，设置配置，并打开门栓（这个门栓仅在第一次初始化有用，下面有一个地方会重新初始化门栓的，那里意味着配置被删除或清空了）
                String s = new String(data);
                setConf(s);
                countDownLatch.countDown();
            }
        }

        public void aWait() {
            // 1、监听节点
            zooKeeper.exists("/conf", this, this, "ABC");
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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
                    // 节点被创建要重新配置获取数据的回调（创建的时候可以设置数据）
                    zooKeeper.getData(path, this, this, "ABC");
                    break;
                case NodeDeleted:
                    // 如果配置被删除了，则相当于重新初始化，门栓要重启
                    setConf("");
                    countDownLatch = new CountDownLatch(1);
                    break;
                case NodeDataChanged:
                    // 节点数据变更的时候要重新配置获取数据的回调
                    zooKeeper.getData(path, this, this, "ABC");
                    break;
                case NodeChildrenChanged:
                    break;
            }
        }
    }

}
