package com.cis555.search.crawler;

import com.cis555.search.util.PersistentStorage;
import com.cis555.search.crawler.info.URLInfo;
import com.cis555.search.crawler.remote.RemoteWorker;
import com.cis555.search.enums.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.cis555.search.util.ObjectSerializer.fromBinary;
import static com.cis555.search.util.ObjectSerializer.toBinary;

/**
 * Dispatching system.
 * Schedule all the tasks, either assign to a local thread or send to the dispatcher.
 */
public class TaskDispatcher {
    private static Logger logger = LoggerFactory.getLogger(TaskDispatcher.class);

    private List<CrawlerWorker> threads;
    private int threadCount;
    private List<PersistentStorage> persistentStorages = new ArrayList<PersistentStorage>();
    private Thread dispatcherThread;
    private RemoteWorker remoteController;
    private String nodeId;
    private List<String> nodeList;

    private boolean isContinue = true;

    private static int queueSize = Integer.parseInt(Constants.INTHREAD_QUEUE_SIZE.value());

    public TaskDispatcher(Crawler crawler) {
        initialize(crawler);
        /**
         * Persistent Queue is used to store TO-DO tasks. It will be periodically
         * checked and part of the tasks will be moved out for execution.
         */
        this.dispatcherThread = new Thread(() -> {
            logger.info("Start dispatcher.");
            try {
                while (this.isContinue) {
                    for (int j = 0; j < this.threadCount; j++) {
                        CrawlerWorker t = threads.get(j);
                        int capacity = queueSize - t.getQueueSize();
                        if (capacity > 200) {
                            int cnt;
                            for (cnt = 0; cnt < capacity - 200; ++cnt) {
                                byte[] raw = persistentStorages.get(j).poll();
                                if (raw == null) {
                                    break;
                                }
                                t.putIntoQueue((CrawlerTask) fromBinary(raw));
                            }
                            persistentStorages.get(j).sync();
                            if (cnt > 0) {
                                logger.info(cnt + " tasks are moved from persistent queue to in-memory queue for thread " + j + " .");
                            }
                        }
                        if (!this.isContinue) {
                            break;
                        }
                    }
                    Thread.sleep(5 * 1000);
                }
            } catch (InterruptedException e) {
                System.err.println("Failed to interrupt the thread");
            }
            logger.info("End dispatcher.");

        }, "Dispatcher");
    }

    private int assignThreadId(CrawlerTask task) {
        int hash = getRandom(task);
        return (hash % threadCount + threadCount) % threadCount;
    }

    private int assignNodeId(CrawlerTask task) {
        int size = nodeList.size() == 0 ? 1 : nodeList.size();
        return getRandom(task) % size;
    }

    private int getRandom(CrawlerTask task) {
        return task.getUrl().getHost().hashCode();
    }

    /**
     * Dispatch to remote node
     */
    public void addTask(CrawlerTask task) {
        if (nodeList.size() > 0) {
            int assigned = this.assignNodeId(task);
            if (!this.nodeId.equals(nodeList.get(assigned))) {
                //If not the same, adding to remote
                this.addTaskToRemote(assigned, task);
                return;
            }
        }
        // Adding to local thread
        int threadId = this.assignThreadId(task);
        this.addToLocal(threadId, task);
    }

    public void getTaskFromRemote(String url) {
        CrawlerTask task = new CrawlerTask(new URLInfo(url));
        int assignThreadId = this.assignThreadId(task);
        this.addToLocal(assignThreadId, task);
    }

    private void addTaskToRemote(int nodeId, CrawlerTask task) {
        List<String> arr = new ArrayList<String>();
        arr.add(task.getUrl().toString());
        remoteController.sendUrls(nodeId, arr);
    }

    private void addToLocal(int threadId, CrawlerTask task) {
        try {
            CrawlerWorker t = threads.get(threadId);
            if (isContinue && t.getQueueSize() < queueSize) {
                t.putIntoQueue(task); // InterruptedException
            } else {
                persistentStorages.get(threadId).offer(toBinary(task));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Save in memory tasks to the persistence queue
     */
    public void saveProgress() {
        for (int j = 0; j < this.threadCount; j++) {
            int cnt = 0;
            CrawlerWorker t = threads.get(j);
            PersistentStorage pq = persistentStorages.get(j);

            Iterator<CrawlerTask> it = t.getTaskIterator();
            while (it.hasNext()) {
                CrawlerTask task = it.next();
                pq.offer(toBinary(task));
                cnt++;
                it.remove();
            }
            pq.sync();
            logger.info(cnt + " tasks have been saved to persistent queue for thread " + j);
        }
        persistentStorages.forEach(e -> e.close());
    }

    public synchronized void start() {
        this.dispatcherThread.start();
    }

    public void stop() {
        this.isContinue = false;
    }

    public final List<Long> getPersistentStorageSize() {
        return persistentStorages.stream().map(e -> e.size()).collect(Collectors.toList());
    }

    private void initialize(Crawler crawler) {
        this.threads = crawler.getThreads();
        this.threadCount = crawler.getThreadCount();
        this.remoteController = crawler.getRemoteController();
        this.nodeList = remoteController.getNames();
        this.nodeId = crawler.getCrawlerIdentifier();
        for (int j = 0; j < threadCount; j++) {
            this.persistentStorages.add(new PersistentStorage("PS" + j));
        }
    }

}
