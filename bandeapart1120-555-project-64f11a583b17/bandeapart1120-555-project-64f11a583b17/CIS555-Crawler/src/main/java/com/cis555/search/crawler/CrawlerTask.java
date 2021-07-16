package com.cis555.search.crawler;

import com.cis555.search.crawler.info.URLInfo;

import java.io.Serializable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * CrawlerTask must be serializable since it will be sent to different
 * crawling nodes through socket. Each CrawlerTask will be stored in the
 * queue and polled when worker node is available.
 */
public class CrawlerTask implements Delayed, Serializable {

    private final URLInfo url;
    private final long timestamp = System.nanoTime();        //creation time
    private int counter = 0;                                //Record the count of redirect
    private long delayedTo = 0L;                            //Delayed to a future timestamp. 0 means now.

    public CrawlerTask(URLInfo url) {
        this.url = url;
        this.delayedTo = System.currentTimeMillis();
    }

    public CrawlerTask(URLInfo url, long delayedTo) {
        this.url = url;
        this.delayedTo = delayedTo;
    }

    public final URLInfo getUrl() {
        return url;
    }

    public final int getCounter() {
        return counter;
    }

    private final void setCounter(int count) {
        this.counter = count;
    }

    public CrawlerTask newRedirectedTask(String dest) {
        if (dest.startsWith("/")) {
            dest = this.getUrl().toRootURLString() + dest;
        }
        CrawlerTask newTask = new CrawlerTask(new URLInfo(dest), this.delayedTo);
        newTask.setCounter(this.counter + 1);
        return newTask;
    }

    @Override
    public int compareTo(Delayed o) {
        int ret = Long.compare(this.getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
        if (ret == 0) {
            return Long.compare(this.getTimestamp(), ((CrawlerTask) o).getTimestamp());
        }
        return ret;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        //Have to override this method
        long diff = this.delayedTo - System.currentTimeMillis();
        return unit.convert(diff, TimeUnit.MILLISECONDS);
    }

    public final void setDelay(long delayedTo) {
        this.delayedTo = delayedTo;
    }

    public final long getTimestamp() {
        return timestamp;
    }

}