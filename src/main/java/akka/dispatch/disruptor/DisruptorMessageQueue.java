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
import akka.dispatch.Envelope;
import akka.dispatch.MessageQueue;
import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;

final class DisruptorMessageQueue implements MessageQueue, DisruptorMessageQueueSemantics {

    private static final EventFactory<ValueEvent> EVENT_FACTORY = new EventFactory<ValueEvent>() {

        @Override
        public ValueEvent newInstance() {
            return new ValueEvent();
        }
    };

    private final Sequence sequence = new Sequence();
    private final RingBuffer<ValueEvent> ringBuffer;
    private final SequenceBarrier sequenceBarrier;
    private final int mask;
    private final Envelope[] buffer;
    private int head;
    private int tail;

    DisruptorMessageQueue(int bufferSize, WaitStrategy waitStrategy) {
        ringBuffer = RingBuffer.createMultiProducer(EVENT_FACTORY, bufferSize, waitStrategy);
        ringBuffer.addGatingSequences(sequence);
        sequenceBarrier = ringBuffer.newBarrier();
        mask = bufferSize - 1;
        buffer = new Envelope[bufferSize];
    }

    @Override
    public void enqueue(ActorRef receiver, Envelope handle) {
        long nextSequence = ringBuffer.next();
        ringBuffer.get(nextSequence).handle = handle;
        ringBuffer.publish(nextSequence);
    }

    @Override
    public Envelope dequeue() {
        if (head != tail) {
            int h = head++ & mask;
            Envelope handle = buffer[h];
            buffer[h] = null;
            return handle;
        }

        long nextSequence = sequence.get() + 1L;
        boolean interrupted = false;
        long availableSequence;
        try {
            while (true) {
                try {
                    availableSequence = sequenceBarrier.waitFor(nextSequence);
                    if (nextSequence <= availableSequence) {
                        break;
                    }
                    Thread.yield();
                } catch (AlertException | InterruptedException ignored) {
                    interrupted = true;
                } catch (TimeoutException ignored) {
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        Envelope handle = ringBuffer.get(nextSequence++).handle;
        int t = tail;
        while (nextSequence <= availableSequence) {
            buffer[t++ & mask] = ringBuffer.get(nextSequence++).handle;
        }

        tail = t;
        sequence.set(availableSequence);
        return handle;
    }

    @Override
    public int numberOfMessages() {
        return 0;
    }

    @Override
    public boolean hasMessages() {
        return head != tail || sequence.get() < sequenceBarrier.getCursor();
    }

    @Override
    public void cleanUp(ActorRef owner, MessageQueue deadLetters) {
        while (hasMessages()) {
            deadLetters.enqueue(owner, dequeue());
        }
    }

    private static final class ValueEvent {

        Envelope handle;
    }
}
