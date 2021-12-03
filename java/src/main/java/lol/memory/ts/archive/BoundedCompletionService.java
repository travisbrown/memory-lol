package lol.memory.ts.archive;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A completion service with a bounded result queue that blocks workers.
 */
final class BoundedCompletionService<V> implements CompletionService<V> {
    private final ExecutorService executor;
    private final BlockingQueue<Future<V>> completionQueue;

    BoundedCompletionService(final ExecutorService executor, final int capacity) {
        this.executor = executor;
        this.completionQueue = new ArrayBlockingQueue<Future<V>>(capacity);
    }

    private final class QueueingFutureTask extends FutureTask<V> {
        QueueingFutureTask(final QueueingCallable wrapped) {
            super(wrapped);
            wrapped.setFutureTask(this);
        }

        QueueingFutureTask(final QueueingRunnable wrapped, V result) {
            super(wrapped, result);
            wrapped.setFutureTask(this);
        }
    }

    private final class QueueingCallable implements Callable<V> {
        private final Callable<V> wrapped;
        private FutureTask<V> task = null;

        QueueingCallable(final Callable<V> wrapped) {
            this.wrapped = wrapped;
        }

        public void setFutureTask(final FutureTask<V> task) {
            this.task = task;
        }

        public V call() throws Exception {
            var result = this.wrapped.call();
            BoundedCompletionService.this.completionQueue.put(this.task);
            return result;
        }
    }

    private final class QueueingRunnable implements Runnable {
        private final Runnable wrapped;
        private FutureTask<V> task = null;

        QueueingRunnable(final Runnable wrapped) {
            this.wrapped = wrapped;
        }

        public void setFutureTask(final FutureTask<V> task) {
            this.task = task;
        }

        public void run() {
            this.wrapped.run();
            try {
                BoundedCompletionService.this.completionQueue.put(this.task);
            } catch (final InterruptedException error) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public Future<V> submit(final Callable<V> task) {
        RunnableFuture<V> future = new QueueingFutureTask(new QueueingCallable(task));
        executor.execute(future);
        return future;
    }

    public Future<V> submit(final Runnable task, final V result) {
        RunnableFuture<V> future = new QueueingFutureTask(new QueueingRunnable(task), result);
        executor.execute(future);
        return future;
    }

    public Future<V> take() throws InterruptedException {
        return this.completionQueue.take();
    }

    public Future<V> poll() {
        return this.completionQueue.poll();
    }

    public Future<V> poll(final long timeout, final TimeUnit unit) throws InterruptedException {
        return this.completionQueue.poll(timeout, unit);
    }
}
