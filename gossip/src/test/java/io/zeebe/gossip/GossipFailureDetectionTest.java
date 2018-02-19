/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.gossip;

import static io.zeebe.test.util.TestUtil.doRepeatedly;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.clustering.gossip.GossipEventType;
import io.zeebe.clustering.gossip.MembershipEventType;
import io.zeebe.gossip.protocol.MembershipEvent;
import io.zeebe.gossip.util.GossipClusterRule;
import io.zeebe.gossip.util.GossipRule;
import io.zeebe.util.sched.clock.ControlledActorClock;
import io.zeebe.util.sched.testing.ActorSchedulerRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class GossipFailureDetectionTest
{
    private static final GossipConfiguration CONFIGURATION = new GossipConfiguration();

    private ControlledActorClock clock = new ControlledActorClock();
    private ActorSchedulerRule actorScheduler = new ActorSchedulerRule(clock);

    private GossipRule gossip1 = new GossipRule(() -> actorScheduler.get(), CONFIGURATION, "localhost", 8001);
    private GossipRule gossip2 = new GossipRule(() -> actorScheduler.get(), CONFIGURATION, "localhost", 8002);
    private GossipRule gossip3 = new GossipRule(() -> actorScheduler.get(), CONFIGURATION, "localhost", 8003);

    @Rule
    public GossipClusterRule cluster = new GossipClusterRule(actorScheduler, gossip1, gossip2, gossip3);

    @Rule
    public Timeout timeout = Timeout.seconds(10);

    @Before
    public void init()
    {
        gossip2.join(gossip1).join();
        gossip3.join(gossip1).join();

        doRepeatedly(() ->
        {
            clock.addTime(CONFIGURATION.getProbeInterval());
        }).until(v ->
        {
            return gossip2.hasMember(gossip3) && gossip3.hasMember(gossip2);
        });

        gossip1.clearReceivedEvents();
        gossip2.clearReceivedEvents();
        gossip3.clearReceivedEvents();
    }

    @Test
    public void shouldSendPingAndAck()
    {
        // when
        doRepeatedly(() ->
        {
            clock.addTime(CONFIGURATION.getProbeInterval());
        }).until(v ->
        {
            return gossip1.receivedEvent(GossipEventType.PING, gossip2) &&
                    gossip1.receivedEvent(GossipEventType.PING, gossip3) &&
                    gossip2.receivedEvent(GossipEventType.PING, gossip1) &&
                    gossip2.receivedEvent(GossipEventType.PING, gossip3) &&
                    gossip3.receivedEvent(GossipEventType.PING, gossip1) &&
                    gossip3.receivedEvent(GossipEventType.PING, gossip2);
        });

        // then
        assertThat(gossip1.receivedEvent(GossipEventType.ACK, gossip2)).isTrue();
        assertThat(gossip1.receivedEvent(GossipEventType.ACK, gossip3)).isTrue();

        assertThat(gossip2.receivedEvent(GossipEventType.ACK, gossip1)).isTrue();
        assertThat(gossip2.receivedEvent(GossipEventType.ACK, gossip3)).isTrue();

        assertThat(gossip3.receivedEvent(GossipEventType.ACK, gossip1)).isTrue();
        assertThat(gossip3.receivedEvent(GossipEventType.ACK, gossip2)).isTrue();
    }

    @Test
    public void shouldSendPingReqAndForwardAck()
    {
        // given
        cluster.interruptConnectionBetween(gossip1, gossip2);

        // when
        doRepeatedly(() ->
        {
            clock.addTime(CONFIGURATION.getProbeInterval());
        }).until(v ->
        {
            return gossip3.receivedEvent(GossipEventType.PING_REQ, gossip1) &&
                    gossip2.receivedEvent(GossipEventType.PING, gossip3) &&
                    gossip3.receivedEvent(GossipEventType.ACK, gossip2) &&
                    gossip1.receivedEvent(GossipEventType.ACK, gossip3);
        });

        // then
        assertThat(gossip2.receivedEvent(GossipEventType.PING, gossip1)).isFalse();
        assertThat(gossip1.receivedEvent(GossipEventType.ACK, gossip2)).isFalse();

        assertThat(gossip1.hasMember(gossip2)).isTrue();
        assertThat(gossip2.hasMember(gossip1)).isTrue();
    }

    @Test
    public void shouldSpreadSuspectEvent()
    {
        // given
        cluster.interruptConnectionBetween(gossip3, gossip1);
        cluster.interruptConnectionBetween(gossip3, gossip2);

        // when
        doRepeatedly(() ->
        {
            clock.addTime(CONFIGURATION.getProbeInterval());
        }).until(v ->
        {
            return gossip1.receivedMembershipEvent(MembershipEventType.SUSPECT, gossip3)
                    && gossip2.receivedMembershipEvent(MembershipEventType.SUSPECT, gossip3);
        });

        // then
        assertThat(gossip1.hasMember(gossip3)).isTrue();
        assertThat(gossip2.hasMember(gossip3)).isTrue();
    }

    @Test
    public void shouldSpreadConfirmEvent()
    {
        // given
        cluster.interruptConnectionBetween(gossip3, gossip1);
        cluster.interruptConnectionBetween(gossip3, gossip2);

        // when
        doRepeatedly(() ->
        {
            clock.addTime(CONFIGURATION.getProbeInterval());
        }).until(v ->
        {
            return gossip1.receivedMembershipEvent(MembershipEventType.CONFIRM, gossip3)
                    && gossip2.receivedMembershipEvent(MembershipEventType.CONFIRM, gossip3);
        });

        // then
        assertThat(gossip1.hasMember(gossip3)).isFalse();
        assertThat(gossip2.hasMember(gossip3)).isFalse();
    }

    @Test
    public void shouldCounterSuspectEventIfAlive()
    {
        // given
        cluster.interruptConnectionBetween(gossip3, gossip1);
        cluster.interruptConnectionBetween(gossip3, gossip2);

        doRepeatedly(() ->
        {
            clock.addTime(CONFIGURATION.getProbeInterval());
        }).until(v ->
        {
            return gossip1.receivedMembershipEvent(MembershipEventType.SUSPECT, gossip3)
                    && gossip2.receivedMembershipEvent(MembershipEventType.SUSPECT, gossip3);
        });

        final MembershipEvent suspectEvent = gossip1.getReceivedMembershipEvents(MembershipEventType.SUSPECT, gossip3)
                .findFirst()
                .get();

        // when
        cluster.reconnect(gossip3, gossip1);
        cluster.reconnect(gossip3, gossip2);

        // then
        doRepeatedly(() ->
        {
            clock.addTime(CONFIGURATION.getProbeInterval());
        }).until(v ->
        {
            return gossip3.receivedMembershipEvent(MembershipEventType.SUSPECT, gossip3) &&
                    gossip1.getReceivedMembershipEvents(MembershipEventType.ALIVE, gossip3)
                           .anyMatch(e -> e.getGossipTerm().isGreaterThan(suspectEvent.getGossipTerm()));
        });
    }

}
