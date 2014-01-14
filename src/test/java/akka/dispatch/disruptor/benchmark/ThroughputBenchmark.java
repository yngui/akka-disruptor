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

package akka.dispatch.disruptor.benchmark;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@FixMethodOrder
public class ThroughputBenchmark extends AbstractBenchmark {

    private final int numberOfClients;
    private final long repeats;
    private final Config config;
    private final String dispatcher1;
    private final String dispatcher2;
    private ActorSystem system;
    private CountDownLatch latch;
    private ActorRef[] destinations;
    private ActorRef[] clients;

    public ThroughputBenchmark(int numberOfClients, long repeats, Config config, String dispatcher1,
                               String dispatcher2) {
        this.numberOfClients = numberOfClients;
        this.repeats = repeats;
        this.config = config;
        this.dispatcher1 = dispatcher1;
        this.dispatcher2 = dispatcher2;
    }

    @Before
    public void setUp() throws Exception {
        system = ActorSystem.create("MySystem", config);
        latch = new CountDownLatch(numberOfClients);
        destinations = new ActorRef[numberOfClients];
        clients = new ActorRef[numberOfClients];

        Props props = Props.create(Destination.class).withDispatcher(dispatcher1);
        long repeatsPerClient = repeats / numberOfClients;

        for (int i = 0; i < numberOfClients; i++) {
            ActorRef destination = system.actorOf(props);
            destinations[i] = destination;
            clients[i] = system.actorOf(Props.create(Client.class, destination, latch, repeatsPerClient)
                    .withDispatcher(dispatcher2));
        }
    }

    @After
    public void tearDown() throws Exception {
        for (int i = 0; i < numberOfClients; i++) {
            system.stop(clients[i]);
            system.stop(destinations[i]);
        }
    }

    @Test
    public void test() throws InterruptedException {
        for (int i = 0; i < numberOfClients; i++) {
            clients[i].tell(Run.INSTANCE, null);
        }
        latch.await();
    }

    @Parameters
    public static List<Object[]> parameters() {
        long repeats = 10000000L;
        Config config1 = ConfigFactory.load("disruptor.conf");
        Config config2 = ConfigFactory.load("bounded.conf");
        Config config3 = ConfigFactory.load("unbounded.conf");
        return Arrays.asList(new Object[][]{
                {1, repeats, config1, "dispatcher1", "dispatcher2"},
                {2, repeats, config1, "dispatcher1", "dispatcher2"},
                {4, repeats, config1, "dispatcher1", "dispatcher2"},
                {8, repeats, config1, "dispatcher1", "dispatcher2"},
                {16, repeats, config1, "dispatcher1", "dispatcher2"},
                {1, repeats, config2, "dispatcher1", "dispatcher2"},
                {2, repeats, config2, "dispatcher1", "dispatcher2"},
                {4, repeats, config2, "dispatcher1", "dispatcher2"},
                {8, repeats, config2, "dispatcher1", "dispatcher2"},
                {16, repeats, config2, "dispatcher1", "dispatcher2"},
                {1, repeats, config3, "dispatcher", "dispatcher"},
                {2, repeats, config3, "dispatcher", "dispatcher"},
                {4, repeats, config3, "dispatcher", "dispatcher"},
                {8, repeats, config3, "dispatcher", "dispatcher"},
                {16, repeats, config3, "dispatcher", "dispatcher"}
        });
    }

    private static final class Destination extends UntypedActor {

        @Override
        public void onReceive(Object message) throws Exception {
            getSender().tell(message, getSelf());
        }
    }

    private static final class Client extends UntypedActor {

        private final ActorRef destination;
        private final CountDownLatch latch;
        private final long repeats;
        private long sent;
        private long received;

        Client(ActorRef destination, CountDownLatch latch, long repeats) {
            this.destination = destination;
            this.latch = latch;
            this.repeats = repeats;
        }

        @Override
        public void onReceive(Object message) throws Exception {
            if (message instanceof Msg) {
                received++;
                if (sent < repeats) {
                    destination.tell(Msg.INSTANCE, getSelf());
                    sent++;
                } else if (received >= repeats) {
                    latch.countDown();
                }
            } else if (message instanceof Run) {
                long n = Math.min(1000L, repeats);
                for (long i = 0L; i < n; i++) {
                    destination.tell(Msg.INSTANCE, getSelf());
                    sent++;
                }
            }
        }
    }

    private static final class Msg {

        static final Msg INSTANCE = new Msg();
    }

    private static final class Run {

        static final Run INSTANCE = new Run();
    }
}
