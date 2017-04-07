package io.searchbox.common;

import com.carrotsearch.randomizedtesting.ThreadFilter;

public class OkHttpThreadsFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread thread) {
        final String threadName = thread.getName();

        return threadName.startsWith("OkHttp") || threadName.startsWith("Okio");
    }

}
