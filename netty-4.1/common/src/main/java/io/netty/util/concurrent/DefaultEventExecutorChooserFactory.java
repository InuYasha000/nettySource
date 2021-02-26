/*
 * Copyright 2016 The Netty Project
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

import io.netty.util.internal.UnstableApi;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 默认 EventExecutorChooser 工厂实现类
 *
 * Default implementation which uses simple round-robin to choose next {@link EventExecutor}.
 */
@UnstableApi
public final class DefaultEventExecutorChooserFactory implements EventExecutorChooserFactory {

    /**
     * 单例
     */
    public static final DefaultEventExecutorChooserFactory INSTANCE = new DefaultEventExecutorChooserFactory();

    private DefaultEventExecutorChooserFactory() { }

    @SuppressWarnings("unchecked")
    @Override
    public EventExecutorChooser newChooser(EventExecutor[] executors) {\
        // 我们以 8 来举个例子。
        // 8 的二进制为 1000 。
        // -8 的二进制使用补码表示。所以，先求反生成反码为 0111 ，然后加一生成补码为 1000 。
        // 8 和 -8 并操作后，还是 8 。
        // 实际上，以 2 为幂次方的数字，都是最高位为 1 ，剩余位为 0 ，所以对应的负数，求完补码还是自己。
        if (isPowerOfTwo(executors.length)) { // 是否为 2 的幂次方
            return new PowerOfTwoEventExecutorChooser(executors);
        } else {
            return new GenericEventExecutorChooser(executors);
        }
    }

    private static boolean isPowerOfTwo(int val) {
        return (val & -val) == val;
    }

    //这个可以看成优化，但我觉得唯一可以说成优化点在于位运算快一点，两者都是单调递增
    private static final class PowerOfTwoEventExecutorChooser implements EventExecutorChooser {

        /**
         * 自增序列
         */
        private final AtomicInteger idx = new AtomicInteger();
        /**
         * EventExecutor 数组
         */
        private final EventExecutor[] executors;

        PowerOfTwoEventExecutorChooser(EventExecutor[] executors) {
            this.executors = executors;
        }

        @Override
        public EventExecutor next() {
            return executors[idx.getAndIncrement() & executors.length - 1];
        }

    }

    //利用idx自增，简单求余
    private static final class GenericEventExecutorChooser implements EventExecutorChooser {

        /**
         * 自增序列
         */
        private final AtomicInteger idx = new AtomicInteger();
        /**
         * EventExecutor 数组
         */
        private final EventExecutor[] executors;

        GenericEventExecutorChooser(EventExecutor[] executors) {
            this.executors = executors;
        }

        @Override
        public EventExecutor next() {
            return executors[Math.abs(idx.getAndIncrement() % executors.length)];
        }

    }

}