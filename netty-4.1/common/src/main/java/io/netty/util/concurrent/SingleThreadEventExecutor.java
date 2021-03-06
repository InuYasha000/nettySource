/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.util.concurrent;

import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.UnstableApi;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * 基于单线程的 EventExecutor 抽象类，即一个 EventExecutor 对应一个线程
 *
 * Abstract base class for {@link OrderedEventExecutor}'s that execute all its submitted tasks in a single thread.
 */
public abstract class SingleThreadEventExecutor extends AbstractScheduledEventExecutor implements OrderedEventExecutor {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(SingleThreadEventExecutor.class);

    /**
     * 默认任务队列最大数量
     */
    static final int DEFAULT_MAX_PENDING_EXECUTOR_TASKS = Math.max(16, SystemPropertyUtil.getInt("io.netty.eventexecutor.maxPendingTasks", Integer.MAX_VALUE));

    //线程状态，因为 thread 的初始化采用延迟启动的方式，只有在第一个任务时，executor 才会执行并创建该线程，从而节省资源。
    private static final int ST_NOT_STARTED = 1; // 未开始
    private static final int ST_STARTED = 2; // 已开始
    private static final int ST_SHUTTING_DOWN = 3; // 正在关闭中
    private static final int ST_SHUTDOWN = 4; // 已关闭isShutdown
    private static final int ST_TERMINATED = 5; // 已经终止

    private static final Runnable WAKEUP_TASK = new Runnable() {
        @Override
        public void run() {
            // Do nothing.
        }
    };
    private static final Runnable NOOP_TASK = new Runnable() {
        @Override
        public void run() {
            // Do nothing.
        }
    };

    /**
     * {@link #state} 字段的原子更新器
     */
    private static final AtomicIntegerFieldUpdater<SingleThreadEventExecutor> STATE_UPDATER =AtomicIntegerFieldUpdater.newUpdater(SingleThreadEventExecutor.class, "state");
    /**
     * {@link #thread} 字段的原子更新器
     */
    private static final AtomicReferenceFieldUpdater<SingleThreadEventExecutor, ThreadProperties> PROPERTIES_UPDATER = AtomicReferenceFieldUpdater.newUpdater(SingleThreadEventExecutor.class, ThreadProperties.class, "threadProperties");

    /**
     * 任务队列
     * addTaskWakesUp 属性，添加任务到 taskQueue 队列时，是否唤醒 thread 线程。
     * maxPendingTasks 属性，最大等待执行任务数量，即 taskQueue 队列大小。
     * rejectedExecutionHandler 属性，拒绝执行处理器。在 taskQueue 队列超过最大任务数量时，怎么拒绝处理新提交的任务。
     *
     * @see #newTaskQueue(int)
     */
    private final Queue<Runnable> taskQueue;
    /**
     * 线程
     * 在 SingleThreadEventExecutor 中，任务是提交到 taskQueue 队列中，而执行在 thread 线程中。
     */
    private volatile Thread thread;
    /**
     * 线程属性
     */
    @SuppressWarnings("unused")
    private volatile ThreadProperties threadProperties;
    /**
     * 执行器
     */
    private final Executor executor;
    /**
     * 线程是否已经打断
     *
     * @see #interruptThread()
     */
    private volatile boolean interrupted;

    /**
     * TODO 1006 EventLoop 优雅关闭
     */
    private final Semaphore threadLock = new Semaphore(0);
    /**
     * TODO 1006 EventLoop 优雅关闭
     */
    private final Set<Runnable> shutdownHooks = new LinkedHashSet<Runnable>();
    /**
     * 添加任务时，是否唤醒线程{@link #thread}
     * 添加任务后，任务是否会自动导致线程唤醒”，但它为false时需要去唤醒
     * 对于 Nio 使用的 NioEventLoop ，它的线程执行任务是基于 Selector 监听感兴趣的事件，
     * 所以当任务添加到 taskQueue 队列中时，线程是无感知的，所以需要调用 #wakeup(boolean inEventLoop) 方法，进行主动的唤醒。
     */
    private final boolean addTaskWakesUp;
    /**
     * 最大等待执行任务数量，即 {@link #taskQueue} 的队列大小
     */
    private final int maxPendingTasks;
    /**
     * 拒绝执行处理器
     *
     * @see #reject()
     * @see #reject(Runnable)
     */
    private final RejectedExecutionHandler rejectedExecutionHandler;

    /**
     * 最后执行时间
     */
    private long lastExecutionTime;

    /**
     * 状态
     * 线程状态。SingleThreadEventExecutor 在实现上，thread 的初始化采用延迟启动的方式，
     * 只有在第一个任务时，executor 才会执行并创建该线程，从而节省资源
     */
    @SuppressWarnings({ "FieldMayBeFinal", "unused" })
    private volatile int state = ST_NOT_STARTED;

    /**
     * TODO 优雅关闭
     */
    private volatile long gracefulShutdownQuietPeriod;
    /**
     * 优雅关闭超时时间，单位：毫秒 TODO 1006 EventLoop 优雅关闭
     */
    private volatile long gracefulShutdownTimeout;
    /**
     * 优雅关闭开始时间，单位：毫秒 TODO 1006 EventLoop 优雅关闭
     */
    private long gracefulShutdownStartTime;

    /**
     * TODO 1006 EventLoop 优雅关闭
     */
    private final Promise<?> terminationFuture = new DefaultPromise<Void>(GlobalEventExecutor.INSTANCE);

    /**
     * Create a new instance
     *
     * @param parent            the {@link EventExecutorGroup} which is the parent of this instance and belongs to it
     * @param threadFactory     the {@link ThreadFactory} which will be used for the used {@link Thread}
     * @param addTaskWakesUp    {@code true} if and only if invocation of {@link #addTask(Runnable)} will wake up the
     *                          executor thread
     */
    protected SingleThreadEventExecutor(
            EventExecutorGroup parent, ThreadFactory threadFactory, boolean addTaskWakesUp) {
        this(parent, new ThreadPerTaskExecutor(threadFactory), addTaskWakesUp);
    }

    /**
     * Create a new instance
     *
     * @param parent            the {@link EventExecutorGroup} which is the parent of this instance and belongs to it
     * @param threadFactory     the {@link ThreadFactory} which will be used for the used {@link Thread}
     * @param addTaskWakesUp    {@code true} if and only if invocation of {@link #addTask(Runnable)} will wake up the
     *                          executor thread
     * @param maxPendingTasks   the maximum number of pending tasks before new tasks will be rejected.
     * @param rejectedHandler   the {@link RejectedExecutionHandler} to use.
     */
    protected SingleThreadEventExecutor(
            EventExecutorGroup parent, ThreadFactory threadFactory,
            boolean addTaskWakesUp, int maxPendingTasks, RejectedExecutionHandler rejectedHandler) {
        this(parent, new ThreadPerTaskExecutor(threadFactory), addTaskWakesUp, maxPendingTasks, rejectedHandler);
    }

    /**
     * Create a new instance
     *
     * @param parent            the {@link EventExecutorGroup} which is the parent of this instance and belongs to it
     * @param executor          the {@link Executor} which will be used for executing
     * @param addTaskWakesUp    {@code true} if and only if invocation of {@link #addTask(Runnable)} will wake up the
     *                          executor thread
     */
    protected SingleThreadEventExecutor(EventExecutorGroup parent, Executor executor, boolean addTaskWakesUp) {
        this(parent, executor, addTaskWakesUp, DEFAULT_MAX_PENDING_EXECUTOR_TASKS, RejectedExecutionHandlers.reject());
    }

    /**
     * Create a new instance
     *
     * @param parent            the {@link EventExecutorGroup} which is the parent of this instance and belongs to it
     * @param executor          the {@link Executor} which will be used for executing
     * @param addTaskWakesUp    {@code true} if and only if invocation of {@link #addTask(Runnable)} will wake up the
     *                          executor thread
     * @param maxPendingTasks   the maximum number of pending tasks before new tasks will be rejected.
     * @param rejectedHandler   the {@link RejectedExecutionHandler} to use.
     */
    protected SingleThreadEventExecutor(EventExecutorGroup parent, Executor executor,
                                        boolean addTaskWakesUp, int maxPendingTasks,
                                        RejectedExecutionHandler rejectedHandler) {
        super(parent);
        this.addTaskWakesUp = addTaskWakesUp;
        this.maxPendingTasks = Math.max(16, maxPendingTasks);
        this.executor = ObjectUtil.checkNotNull(executor, "executor");
        taskQueue = newTaskQueue(this.maxPendingTasks);
        rejectedExecutionHandler = ObjectUtil.checkNotNull(rejectedHandler, "rejectedHandler");
    }

    /**
     * @deprecated Please use and override {@link #newTaskQueue(int)}.
     */
    @Deprecated
    protected Queue<Runnable> newTaskQueue() {
        return newTaskQueue(maxPendingTasks);
    }

    /**
     * Create a new {@link Queue} which will holds the tasks to execute. This default implementation will return a
     * {@link LinkedBlockingQueue} but if your sub-class of {@link SingleThreadEventExecutor} will not do any blocking
     * calls on the this {@link Queue} it may make sense to {@code @Override} this and return some more performant
     * implementation that does not support blocking operations at all.
     */
    protected Queue<Runnable> newTaskQueue(int maxPendingTasks) {
        return new LinkedBlockingQueue<Runnable>(maxPendingTasks);
    }

    /**
     * Interrupt the current running {@link Thread}.
     */
    protected void interruptThread() {
        Thread currentThread = thread;
        // 线程不存在，则标记线程被打断
        if (currentThread == null) {
            interrupted = true;
        // 打断线程
        } else {
            currentThread.interrupt();
        }
    }

    /**
     * @see Queue#poll()
     */
    protected Runnable pollTask() {
        assert inEventLoop();
        return pollTaskFrom(taskQueue);
    }

    protected static Runnable pollTaskFrom(Queue<Runnable> taskQueue) {
        for (;;) {
            // 获得并移除队首元素。如果获得不到，返回 null
            Runnable task = taskQueue.poll();
            // 忽略 WAKEUP_TASK 任务，因为是空任务
            if (task == WAKEUP_TASK) {
                continue;
            }
            return task;
        }
    }

    /**
     * Take the next {@link Runnable} from the task queue and so will block if no task is currently present.
     * <p>
     * Be aware that this method will throw an {@link UnsupportedOperationException} if the task queue, which was
     * created via {@link #newTaskQueue()}, does not implement {@link BlockingQueue}.
     * </p>
     *
     * @return {@code null} if the executor thread has been interrupted or waken up.
     */
    protected Runnable takeTask() {
        assert inEventLoop();
        if (!(taskQueue instanceof BlockingQueue)) {
            throw new UnsupportedOperationException();
        }

        BlockingQueue<Runnable> taskQueue = (BlockingQueue<Runnable>) this.taskQueue;
        for (;;) {
            ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
            if (scheduledTask == null) {
                Runnable task = null;
                try {
                    task = taskQueue.take();
                    if (task == WAKEUP_TASK) {
                        task = null;
                    }
                } catch (InterruptedException e) {
                    // Ignore
                }
                return task;
            } else {
                long delayNanos = scheduledTask.delayNanos();
                Runnable task = null;
                if (delayNanos > 0) {
                    try {
                        task = taskQueue.poll(delayNanos, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        // Waken up.
                        return null;
                    }
                }
                if (task == null) {
                    // We need to fetch the scheduled tasks now as otherwise there may be a chance that
                    // scheduled tasks are never executed if there is always one task in the taskQueue.
                    // This is for example true for the read task of OIO Transport
                    // See https://github.com/netty/netty/issues/1614
                    fetchFromScheduledTaskQueue();
                    task = taskQueue.poll();
                }

                if (task != null) {
                    return task;
                }
            }
        }
    }

    // 将定时任务队列 scheduledTaskQueue 到达可执行的任务，添加到任务队列 taskQueue 中
    private boolean fetchFromScheduledTaskQueue() {
        // 获得当前时间
        long nanoTime = AbstractScheduledEventExecutor.nanoTime();
        // 获得指定时间内，定时任务队列**首个**可执行的任务，并且从队列中移除。
        Runnable scheduledTask  = pollScheduledTask(nanoTime);
        // 不断从定时任务队列中，获得
        while (scheduledTask != null) {
            // 将定时任务添加到 taskQueue 中。若添加失败，则结束循环，返回 false ，表示未获取完所有课执行的定时任务
            if (!taskQueue.offer(scheduledTask)) {
                // 将定时任务添加回 scheduledTaskQueue 中
                // No space left in the task queue add it back to the scheduledTaskQueue so we pick it up again.
                scheduledTaskQueue().add((ScheduledFutureTask<?>) scheduledTask);
                return false;
            }
            // 获得指定时间内，定时任务队列**首个**可执行的任务，并且从队列中移除。
            scheduledTask  = pollScheduledTask(nanoTime);
        }
        // 返回 true ，表示获取完所有可执行的定时任务
        return true;
    }

    /**
     * @see Queue#peek()
     */
    protected Runnable peekTask() {
        assert inEventLoop(); // 仅允许在 EventLoop 线程中执行
        return taskQueue.peek();
    }

    /**
     * @see Queue#isEmpty()
     */
    protected boolean hasTasks() {
        assert inEventLoop(); // 仅允许在 EventLoop 线程中执行
        return !taskQueue.isEmpty();
    }

    /**
     * Return the number of tasks that are pending for processing.
     *
     * <strong>Be aware that this operation may be expensive as it depends on the internal implementation of the
     * SingleThreadEventExecutor. So use it was care!</strong>
     */
    public int pendingTasks() {
        return taskQueue.size();
    }

    /**
     * Add a task to the task queue, or throws a {@link RejectedExecutionException} if this instance was shutdown
     * before.
     */
    protected void addTask(Runnable task) {
        if (task == null) {
            throw new NullPointerException("task");
        }
        // 添加任务到队列
        if (!offerTask(task)) {
            // 添加失败，则拒绝任务
            reject(task);
        }
    }

    /**
     * 添加任务到队列中。若添加失败，则返回 false
     *
     * @param task 任务
     * @return 添加任务是否成功
     */
    final boolean offerTask(Runnable task) {
        // 关闭时，拒绝任务
        if (isShutdown()) {
            reject();
        }
        // 添加任务到队列
        return taskQueue.offer(task);
    }

    /**
     * @see Queue#remove(Object)
     */
    protected boolean removeTask(Runnable task) {
        if (task == null) {
            throw new NullPointerException("task");
        }
        return taskQueue.remove(task);
    }

    /**
     * Poll all tasks from the task queue and run them via {@link Runnable#run()} method.
     *
     * @return {@code true} if and only if at least one task was run
     */
    protected boolean runAllTasks() {
        assert inEventLoop();
        boolean fetchedAll;
        boolean ranAtLeastOne = false; // 是否执行过任务

        do {
            // 从定时任务获得到时间的任务
            // fetchedAll 返回 true表示执行完所有任务，返回false表示添加失败，因为这里再次尝试
            // 一般情况来说会不断卡在里面while循环，直到所有的任务都执行完，即使没有执行完（比如添加队列失败，因为taskQueue是有大小限制的）也会返回false
            // 所以外面这个循环和里面的循环会保证一定会执行完所有任务

            // 这里有3个循环：
            // 1:fetchFromScheduledTaskQueue（）的循环保证将定时任务队列 scheduledTaskQueue 到达可执行的任务，添加到任务队列 taskQueue 中
            // 2:runAllTasksFrom()的循环保证执行完任务队列 taskQueue 所有任务
            // 3:最外层循环保证第一个循环添加队列失败后再次添加
            fetchedAll = fetchFromScheduledTaskQueue();
            // 执行任务队列中的所有任务
            if (runAllTasksFrom(taskQueue)) {
                // 若有任务执行，则标记为 true
                ranAtLeastOne = true;
            }
        } while (!fetchedAll); // keep on processing until we fetched all scheduled tasks.

        // 如果执行过任务，则设置最后执行时间
        if (ranAtLeastOne) {
            lastExecutionTime = ScheduledFutureTask.nanoTime();
        }

        // 执行所有任务完成的后续方法
        afterRunningAllTasks();
        return ranAtLeastOne;
    }

    /**
     * Runs all tasks from the passed {@code taskQueue}.
     *
     * @param taskQueue To poll and execute all tasks.
     *
     * @return {@code true} if at least one task was executed.
     */
    protected final boolean runAllTasksFrom(Queue<Runnable> taskQueue) {
        // 获得队头的任务
        Runnable task = pollTaskFrom(taskQueue);
        // 获取不到，结束执行，返回 false
        if (task == null) {
            return false;
        }
        for (;;) {
            // 执行任务
            safeExecute(task);
            // 获得队头的任务
            task = pollTaskFrom(taskQueue);
            // 获取不到，结束执行，返回 true
            if (task == null) {
                return true;
            }
        }
    }

    /**
     * Poll all tasks from the task queue and run them via {@link Runnable#run()} method.  This method stops running
     * the tasks in the task queue and returns if it ran longer than {@code timeoutNanos}.
     *
     * @return 是否有执行到任务
     */
    protected boolean runAllTasks(long timeoutNanos) {
        // 从定时任务获得到时间的任务
        // 将定时任务队列 scheduledTaskQueue 到达可执行的任务，添加到任务队列 taskQueue 中
        fetchFromScheduledTaskQueue();
        // 获得队头的任务
        Runnable task = pollTask();
        // 获取不到，结束执行
        if (task == null) {
            // 执行所有任务完成的后续方法
            afterRunningAllTasks();
            return false;
        }

        // 计算执行任务截止时间
        final long deadline = ScheduledFutureTask.nanoTime() + timeoutNanos;
        long runTasks = 0; // 执行任务计数
        long lastExecutionTime;
        // 循环执行任务
        for (;;) {
            // 执行任务
            safeExecute(task);

            // 计数 +1
            runTasks ++;

            // 每隔 64 个任务检查一次时间，因为 nanoTime() 是相对费时的操作
            // 64 这个值当前是硬编码的，无法配置，可能会成为一个问题。
            // Check timeout every 64 tasks because nanoTime() is relatively expensive.
            // XXX: Hard-coded value - will make it configurable if it is really a problem.
            if ((runTasks & 0x3F) == 0) {
                // 重新获得时间
                lastExecutionTime = ScheduledFutureTask.nanoTime();
                // 超过任务截止时间，结束
                if (lastExecutionTime >= deadline) {
                    break;
                }
            }

            // 获得队头的任务
            task = pollTask();
            // 获取不到，结束执行
            if (task == null) {
                // 重新获得时间
                lastExecutionTime = ScheduledFutureTask.nanoTime();
                break;
            }
        }

        // 执行所有任务完成的后续方法
        afterRunningAllTasks();

        // 设置最后执行时间
        this.lastExecutionTime = lastExecutionTime;
        return true;
    }

    /**
     * Invoked before returning from {@link #runAllTasks()} and {@link #runAllTasks(long)}.
     */
    @UnstableApi
    protected void afterRunningAllTasks() { }

    /**
     * Returns the amount of time left until the scheduled task with the closest dead line is executed.
     * 返回的为下一个定时任务距离现在的时间，如果不存在定时任务，则默认返回 1000 ms
     */
    protected long delayNanos(long currentTimeNanos) {
        ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
        if (scheduledTask == null) {
            return SCHEDULE_PURGE_INTERVAL;
        }

        return scheduledTask.delayNanos(currentTimeNanos);
    }

    /**
     * Updates the internal timestamp that tells when a submitted task was executed most recently.
     * {@link #runAllTasks()} and {@link #runAllTasks(long)} updates this timestamp automatically, and thus there's
     * usually no need to call this method.  However, if you take the tasks manually using {@link #takeTask()} or
     * {@link #pollTask()}, you have to call this method at the end of task execution loop for accurate quiet period
     * checks.
     */
    protected void updateLastExecutionTime() {
        lastExecutionTime = ScheduledFutureTask.nanoTime();
    }

    /**
     * 执行
     */
    protected abstract void run();

    /**
     * Do nothing, sub-classes may override
     */
    protected void cleanup() {
        // NOOP
    }

    protected void wakeup(boolean inEventLoop) {
        if (!inEventLoop
                || state == ST_SHUTTING_DOWN) { // TODO 1006 EventLoop 优雅关闭
            // Use offer as we actually only need this to unblock the thread and if offer fails we do not care as there
            // is already something in the queue.
            taskQueue.offer(WAKEUP_TASK);
        }
    }

    @Override
    public boolean inEventLoop(Thread thread) {
        return thread == this.thread;
    }

    /**
     * Add a {@link Runnable} which will be executed on shutdown of this instance
     */
    public void addShutdownHook(final Runnable task) {
        if (inEventLoop()) {
            shutdownHooks.add(task);
        } else {
            execute(new Runnable() {
                @Override
                public void run() {
                    shutdownHooks.add(task);
                }
            });
        }
    }

    /**
     * Remove a previous added {@link Runnable} as a shutdown hook
     */
    public void removeShutdownHook(final Runnable task) {
        if (inEventLoop()) {
            shutdownHooks.remove(task);
        } else {
            execute(new Runnable() {
                @Override
                public void run() {
                    shutdownHooks.remove(task);
                }
            });
        }
    }

    private boolean runShutdownHooks() {
        boolean ran = false;
        // Note shutdown hooks can add / remove shutdown hooks.
        while (!shutdownHooks.isEmpty()) {
            List<Runnable> copy = new ArrayList<Runnable>(shutdownHooks);
            shutdownHooks.clear();
            for (Runnable task: copy) {
                try {
                    task.run();
                } catch (Throwable t) {
                    logger.warn("Shutdown hook raised an exception.", t);
                } finally {
                    ran = true;
                }
            }
        }

        if (ran) {
            lastExecutionTime = ScheduledFutureTask.nanoTime();
        }

        return ran;
    }

    //优雅关闭相关
    // 我们总结一下，调用shutdown()方法从循环跳出的条件有：
    //(1).执行完普通任务
    //(2).没有普通任务，执行完shutdownHook任务
    //(3).既没有普通任务也没有shutdownHook任务
    //调用shutdownGracefully()方法从循环跳出的条件有：
    //(1).执行完普通任务且静默时间为0
    //(2).没有普通任务，执行完shutdownHook任务且静默时间为0
    //(3).静默期间没有任务提交
    //(4).优雅关闭截止时间已到
    //注意上面所列的条件之间是或的关系，也就是说满足任意一条就会从EventLoop循环中跳出。我们可以将静默时间看为一段观察期，在此期间如果没有任务执行，说明可以跳出循环；如果此期间有任务执行，执行完后立即进入下一个观察期继续观察；如果连续多个观察期一直有任务执行，那么截止时间到则跳出循环
    @Override
    public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
        if (quietPeriod < 0) {
            throw new IllegalArgumentException("quietPeriod: " + quietPeriod + " (expected >= 0)");
        }
        if (timeout < quietPeriod) {
            throw new IllegalArgumentException(
                    "timeout: " + timeout + " (expected >= quietPeriod (" + quietPeriod + "))");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }

        if (isShuttingDown()) {
            return terminationFuture();// 正在关闭阻止其他线程
        }

        boolean inEventLoop = inEventLoop();
        boolean wakeup;
        int oldState;
        for (;;) {
            if (isShuttingDown()) {
                return terminationFuture();// 正在关闭阻止其他线程
            }
            int newState;
            wakeup = true;
            oldState = state;
            if (inEventLoop) {
                newState = ST_SHUTTING_DOWN;
            } else {
                switch (oldState) {
                    case ST_NOT_STARTED:
                    case ST_STARTED:
                        newState = ST_SHUTTING_DOWN;
                        break;
                    default:
                        newState = oldState;
                        wakeup = false;
                }
            }
            if (STATE_UPDATER.compareAndSet(this, oldState, newState)) {// 保证只有一个线程将oldState修改为newState
                break;
            }
        }
        gracefulShutdownQuietPeriod = unit.toNanos(quietPeriod);
        gracefulShutdownTimeout = unit.toNanos(timeout);

        if (oldState == ST_NOT_STARTED) {
            try {
                //启动线程可以完整走一遍正常流程并且可以处理添加到队列中的任务以及IO事件
                doStartThread();
            } catch (Throwable cause) {
                STATE_UPDATER.set(this, ST_TERMINATED);
                terminationFuture.tryFailure(cause);

                if (!(cause instanceof Exception)) {
                    // Also rethrow as it may be an OOME for example
                    PlatformDependent.throwException(cause);
                }
                return terminationFuture;
            }
        }

        if (wakeup) {
            wakeup(inEventLoop);
        }

        return terminationFuture();
    }

    @Override
    public Future<?> terminationFuture() {
        return terminationFuture;
    }

    @Override
    @Deprecated
    public void shutdown() {
        if (isShutdown()) {
            return;
        }

        boolean inEventLoop = inEventLoop();
        boolean wakeup;
        int oldState;
        for (;;) {
            if (isShuttingDown()) {
                return;
            }
            int newState;
            wakeup = true;
            oldState = state;
            if (inEventLoop) {
                newState = ST_SHUTDOWN;
            } else {
                switch (oldState) {
                    case ST_NOT_STARTED:
                    case ST_STARTED:
                    case ST_SHUTTING_DOWN:
                        newState = ST_SHUTDOWN;
                        break;
                    default:
                        newState = oldState;
                        wakeup = false;
                }
            }
            if (STATE_UPDATER.compareAndSet(this, oldState, newState)) {
                break;
            }
        }

        if (oldState == ST_NOT_STARTED) {
            try {
                doStartThread();
            } catch (Throwable cause) {
                STATE_UPDATER.set(this, ST_TERMINATED);
                terminationFuture.tryFailure(cause);

                if (!(cause instanceof Exception)) {
                    // Also rethrow as it may be an OOME for example
                    PlatformDependent.throwException(cause);
                }
                return;
            }
        }

        if (wakeup) {
            wakeup(inEventLoop);
        }
    }

    @Override
    public boolean isShuttingDown() {
        return state >= ST_SHUTTING_DOWN;
    }

    @Override
    public boolean isShutdown() {
        return state >= ST_SHUTDOWN;
    }

    @Override
    public boolean isTerminated() {
        return state == ST_TERMINATED;
    }

    /**
     * Confirm that the shutdown if the instance should be done now!
     */
    protected boolean confirmShutdown() {
        if (!isShuttingDown()) {
            return false;
        }

        if (!inEventLoop()) {
            throw new IllegalStateException("must be invoked from an event loop");
        }

        cancelScheduledTasks();// 取消调度任务

        if (gracefulShutdownStartTime == 0) {// 优雅关闭开始时间，这也是一个标记
            gracefulShutdownStartTime = ScheduledFutureTask.nanoTime();
        }

        // 执行完普通任务或者没有普通任务时执行完shutdownHook任务
        if (runAllTasks() || runShutdownHooks()) {
            if (isShutdown()) {
                // Executor shut down - no new tasks anymore.
                return true;
            }

            // There were tasks in the queue. Wait a little bit more until no tasks are queued for the quiet period or
            // terminate if the quiet period is 0.
            // See https://github.com/netty/netty/issues/4241
            if (gracefulShutdownQuietPeriod == 0) {
                return true;
            }
            wakeup(true);// 优雅关闭但有未执行任务，唤醒线程执行
            return false;
        }

        final long nanoTime = ScheduledFutureTask.nanoTime();

        // shutdown()方法调用直接返回，优雅关闭截止时间到也返回
        if (isShutdown() || nanoTime - gracefulShutdownStartTime > gracefulShutdownTimeout) {
            return true;
        }

        // 在静默期间每100ms唤醒线程执行期间提交的任务
        if (nanoTime - lastExecutionTime <= gracefulShutdownQuietPeriod) {
            // Check if any tasks were added to the queue every 100ms.
            // TODO: Change the behavior of takeTask() so that it returns on timeout.
            wakeup(true);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // Ignore
            }

            return false;
        }

        // No tasks were added for last quiet period - hopefully safe to shut down.
        // (Hopefully because we really cannot make a guarantee that there will be no execute() calls by a user.)
        // 静默时间内没有任务提交，可以优雅关闭，此时若用户又提交任务则不会被执行
        return true;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        if (unit == null) {
            throw new NullPointerException("unit");
        }

        if (inEventLoop()) {
            throw new IllegalStateException("cannot await termination of the current thread");
        }

        if (threadLock.tryAcquire(timeout, unit)) {
            threadLock.release();
        }

        return isTerminated();
    }

    @Override
    public void execute(Runnable task) {
        if (task == null) {
            throw new NullPointerException("task");
        }

        // 获得当前是否在 EventLoop 的线程中
        boolean inEventLoop = inEventLoop();
        // 添加到任务队列
        addTask(task);
        if (!inEventLoop) {
            // 创建线程
            startThread();
            // 若已经关闭，移除任务，并进行拒绝
            if (isShutdown() && removeTask(task)) {
                reject();
            }
        }

        // 唤醒线程
        if (!addTaskWakesUp && wakesUpForTask(task)) {
            wakeup(inEventLoop);
        }
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        throwIfInEventLoop("invokeAny");
        return super.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        throwIfInEventLoop("invokeAny");
        return super.invokeAny(tasks, timeout, unit);
    }

    @Override
    public <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        throwIfInEventLoop("invokeAll");
        return super.invokeAll(tasks);
    }

    @Override
    public <T> List<java.util.concurrent.Future<T>> invokeAll(
            Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        throwIfInEventLoop("invokeAll");
        return super.invokeAll(tasks, timeout, unit);
    }

    private void throwIfInEventLoop(String method) {
        if (inEventLoop()) {
            throw new RejectedExecutionException("Calling " + method + " from within the EventLoop is not allowed");
        }
    }

    /**
     * Returns the {@link ThreadProperties} of the {@link Thread} that powers the {@link SingleThreadEventExecutor}.
     * If the {@link SingleThreadEventExecutor} is not started yet, this operation will start it and block until the
     * it is fully started.
     */
    public final ThreadProperties threadProperties() {
        ThreadProperties threadProperties = this.threadProperties;
        if (threadProperties == null) {
            Thread thread = this.thread;
            if (thread == null) {
                assert !inEventLoop();
                // 提交空任务，促使 execute 方法执行
                submit(NOOP_TASK).syncUninterruptibly();
                // 获得线程
                thread = this.thread;
                assert thread != null;
            }

            // 创建 DefaultThreadProperties 对象
            threadProperties = new DefaultThreadProperties(thread);
            // CAS 修改 threadProperties 属性
            if (!PROPERTIES_UPDATER.compareAndSet(this, null, threadProperties)) {
                threadProperties = this.threadProperties;
            }
        }

        return threadProperties;
    }

    @SuppressWarnings("unused")
    protected boolean wakesUpForTask(Runnable task) {
        return true;
    }

    protected static void reject() {
        throw new RejectedExecutionException("event executor terminated");
    }

    /**
     * Offers the task to the associated {@link RejectedExecutionHandler}.
     *
     * @param task to reject.
     */
    protected final void reject(Runnable task) {
        rejectedExecutionHandler.rejected(task, this);
    }

    // ScheduledExecutorService implementation

    private static final long SCHEDULE_PURGE_INTERVAL = TimeUnit.SECONDS.toNanos(1);

    private void startThread() {
        if (state == ST_NOT_STARTED) {
            if (STATE_UPDATER.compareAndSet(this, ST_NOT_STARTED, ST_STARTED)) {
                try {
                    doStartThread();
                } catch (Throwable cause) {
                    STATE_UPDATER.set(this, ST_NOT_STARTED);
                    PlatformDependent.throwException(cause);
                }
            }
        }
    }

    private void doStartThread() {
        assert thread == null;
        executor.execute(new Runnable() {

            @Override
            public void run() {
                // 记录当前线程
                thread = Thread.currentThread();

                // 如果当前线程已经被标记打断，则进行打断操作。
                if (interrupted) {
                    thread.interrupt();
                }

                boolean success = false; // 是否执行成功

                // 更新最后执行时间
                updateLastExecutionTime();
                try {
                    // 执行任务
                    SingleThreadEventExecutor.this.run();
                    success = true; // 标记执行成功
                } catch (Throwable t) {
                    logger.warn("Unexpected exception from an event executor: ", t);
                } finally {
                    // TODO 1006 EventLoop 优雅关闭
                    for (;;) {
                        int oldState = state;
                        if (oldState >= ST_SHUTTING_DOWN || STATE_UPDATER.compareAndSet(
                                SingleThreadEventExecutor.this, oldState, ST_SHUTTING_DOWN)) {
                            break;
                        }
                    }

                    // TODO 1006 EventLoop 优雅关闭
                    // Check if confirmShutdown() was called at the end of the loop.
                    if (success && gracefulShutdownStartTime == 0) {
                        if (logger.isErrorEnabled()) {
                            logger.error("Buggy " + EventExecutor.class.getSimpleName() + " implementation; " +
                                    SingleThreadEventExecutor.class.getSimpleName() + ".confirmShutdown() must " +
                                    "be called before run() implementation terminates.");
                        }
                    }

                    // TODO 1006 EventLoop 优雅关闭
                    try {
                        // Run all remaining tasks and shutdown hooks.
                        for (;;) {
                            if (confirmShutdown()) {
                                break;
                            }
                        }
                    } finally {
                        try {
                            cleanup(); // 清理，释放资源
                        } finally {
                            STATE_UPDATER.set(SingleThreadEventExecutor.this, ST_TERMINATED);
                            threadLock.release();
                            if (!taskQueue.isEmpty()) {
                                if (logger.isWarnEnabled()) {
                                    logger.warn("An event executor terminated with " +
                                            "non-empty task queue (" + taskQueue.size() + ')');
                                }
                            }

                            terminationFuture.setSuccess(null);
                        }
                    }
                }

            }
        });
    }

    private static final class DefaultThreadProperties implements ThreadProperties {

        private final Thread t;

        DefaultThreadProperties(Thread t) {
            this.t = t;
        }

        @Override
        public State state() {
            return t.getState();
        }

        @Override
        public int priority() {
            return t.getPriority();
        }

        @Override
        public boolean isInterrupted() {
            return t.isInterrupted();
        }

        @Override
        public boolean isDaemon() {
            return t.isDaemon();
        }

        @Override
        public String name() {
            return t.getName();
        }

        @Override
        public long id() {
            return t.getId();
        }

        @Override
        public StackTraceElement[] stackTrace() {
            return t.getStackTrace();
        }

        @Override
        public boolean isAlive() {
            return t.isAlive();
        }

    }
}