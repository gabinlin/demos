import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZKLock {
    private ZooKeeper zooKeeper;

    @Before
    public void before() throws IOException {
        zooKeeper = new ZooKeeper("127.0.0.1:2181/locks", 1000,
                watchedEvent -> System.out.println(watchedEvent.toString()));
    }

    @Test
    public void testLock() {
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                String threadName = Thread.currentThread().getName();
                MyWatch watchCallBack = new MyWatch(zooKeeper, threadName);
                watchCallBack.tryLock();
                System.out.println(watchCallBack.getPathName() + ":" + threadName + " working...");
                watchCallBack.unLock();
            }).start();
        }
        while (true) {

        }
    }

    class MyWatch implements Watcher, AsyncCallback.StringCallback, AsyncCallback.ChildrenCallback, AsyncCallback.StatCallback {
        ZooKeeper zk;
        String threadName;
        CountDownLatch latch = new CountDownLatch(1);
        String path = "/lock";
        String pathName;

        public MyWatch(ZooKeeper zk, String threadName) {
            this.zk = zk;
            this.threadName = threadName;
        }

        public String getPathName() {
            return pathName;
        }

        public void setPathName(String pathName) {
            this.pathName = pathName;
        }

        public void tryLock() {
            try {
                zk.create(path, threadName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, this, "ABC");
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void unLock() {
            try {
                System.out.println("delete" + pathName);
                zk.delete(pathName, -1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void process(WatchedEvent event) {
            switch (event.getType()) {
                case None:
                    break;
                case NodeCreated:
                    break;
                case NodeDeleted:
                    System.out.println("deleteNode" + pathName);
                    // 节点被删除了，就重新设置监听
                    zk.getChildren("/", false, this, "ABC");
                    break;
                case NodeDataChanged:
                    break;
                case NodeChildrenChanged:
                    break;
            }
        }

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            if (name != null) {
                pathName = name;
                zk.getChildren("/", this, this, "ABC");
            }
        }

        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {

        }

        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            Collections.sort(children);
            int i = children.indexOf(pathName.substring(1));
            if (i == 0) {
                System.out.println(threadName + " i am first....");
                try {
                    zk.setData("/", threadName.getBytes(), -1);
                    latch.countDown();

                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                // 仅仅这边设置了watch，会导致如果第一个获取锁的线程执行完之后，才执行到第二个线程的这个方法，会导致永远也通知不到第二个等待锁的线程
                zk.exists("/" + children.get(i - 1), this, this, "ABC");
            }
        }
    }

}
