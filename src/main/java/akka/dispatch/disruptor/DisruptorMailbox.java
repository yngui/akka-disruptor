/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2013 Igor Konev
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package akka.dispatch.disruptor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.MailboxType;
import akka.dispatch.MessageQueue;
import akka.dispatch.ProducesMessageQueue;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.typesafe.config.Config;
import scala.Option;

import static akka.actor.ActorSystem.Settings;

public final class DisruptorMailbox implements MailboxType, ProducesMessageQueue<DisruptorMessageQueue> {

    private final int bufferSize;

    public DisruptorMailbox(Settings settings, Config config) {
        int capacity = config.getInt("mailbox-capacity");
        if (capacity < 1) {
            throw new IllegalArgumentException("Mailbox capacity must not be less than 1");
        }
        if (Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("Mailbox capacity must be a power of 2");
        }
        bufferSize = capacity;
    }

    @Override
    public MessageQueue create(Option<ActorRef> owner, Option<ActorSystem> system) {
        return new DisruptorMessageQueue(bufferSize, new SleepingWaitStrategy());
    }
}
