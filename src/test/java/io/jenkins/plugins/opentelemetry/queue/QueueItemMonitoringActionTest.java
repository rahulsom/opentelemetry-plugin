/*
 * Copyright The Original Author or Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.jenkins.plugins.opentelemetry.queue;

import static io.jenkins.plugins.opentelemetry.semconv.ExtendedJenkinsAttributes.*;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class QueueItemMonitoringActionTest {

    // ── happy path ───────────────────────────────────────────────────────────

    @Test
    void getPhases_emptyBeforeAnyPhaseStarts() {
        var action = new QueueItemMonitoringAction();
        assertThat(action.getPhases().isEmpty(), is(true));
    }

    @Test
    void singlePhaseRecordedWithCorrectFields() {
        var action = new QueueItemMonitoringAction();
        long before = System.currentTimeMillis();
        action.startPhase(JENKINS_JOB_SPAN_PHASE_QUEUE_WAITING_NAME, "Quiet period", null);
        action.endCurrentPhase();
        long after = System.currentTimeMillis();

        var phases = action.getPhases();
        assertThat(phases.size(), is(1));
        var p = phases.get(0);
        assertThat(p.phaseName(), is(JENKINS_JOB_SPAN_PHASE_QUEUE_WAITING_NAME));
        assertThat(p.reason(), is("Quiet period"));
        assertThat(p.label(), is(nullValue()));
        assertTrue(p.startMillis() >= before && p.startMillis() <= after);
        assertTrue(p.endMillis() >= p.startMillis());
    }

    @Test
    void labelStoredAndReturnedPerPhase() {
        var action = new QueueItemMonitoringAction();
        action.startPhase(JENKINS_JOB_SPAN_PHASE_QUEUE_BUILDABLE_NAME, "Waiting", "linux");
        action.endCurrentPhase();

        assertThat(action.getPhases().get(0).label(), is("linux"));
    }

    @Test
    void multipleSequentialPhasesAllRecorded() {
        var action = new QueueItemMonitoringAction();

        action.startPhase(JENKINS_JOB_SPAN_PHASE_QUEUE_WAITING_NAME, "quiet period", null);
        action.endCurrentPhase();
        action.startPhase(JENKINS_JOB_SPAN_PHASE_QUEUE_BLOCKED_NAME, "lock held", "linux");
        action.endCurrentPhase();
        action.startPhase(JENKINS_JOB_SPAN_PHASE_QUEUE_BUILDABLE_NAME, "waiting for executor", "linux");
        action.endCurrentPhase();

        var phases = action.getPhases();
        assertThat(phases.size(), is(3));
        assertThat(phases.get(0).phaseName(), is(JENKINS_JOB_SPAN_PHASE_QUEUE_WAITING_NAME));
        assertThat(phases.get(1).phaseName(), is(JENKINS_JOB_SPAN_PHASE_QUEUE_BLOCKED_NAME));
        assertThat(phases.get(2).phaseName(), is(JENKINS_JOB_SPAN_PHASE_QUEUE_BUILDABLE_NAME));
    }

    @Test
    void phasesAreInChronologicalOrder() {
        var action = new QueueItemMonitoringAction();
        action.startPhase(JENKINS_JOB_SPAN_PHASE_QUEUE_WAITING_NAME, null, null);
        action.endCurrentPhase();
        action.startPhase(JENKINS_JOB_SPAN_PHASE_QUEUE_BUILDABLE_NAME, null, null);
        action.endCurrentPhase();

        var phases = action.getPhases();
        assertTrue(phases.get(0).startMillis() <= phases.get(1).startMillis());
    }

    // ── edge cases ───────────────────────────────────────────────────────────

    @Test
    void endCurrentPhase_noopWhenNoPhaseOpen() {
        var action = new QueueItemMonitoringAction();
        assertDoesNotThrow(action::endCurrentPhase);
        assertThat(action.getPhases().isEmpty(), is(true));
    }

    @Test
    void getPhases_doesNotIncludeOpenPhase() {
        var action = new QueueItemMonitoringAction();
        action.startPhase(JENKINS_JOB_SPAN_PHASE_QUEUE_WAITING_NAME, null, null);
        // phase started but not ended
        assertThat(action.getPhases().isEmpty(), is(true));
    }

    @Test
    void startPhase_whenPhaseAlreadyOpen_closesItAndStartsNew() {
        var action = new QueueItemMonitoringAction();
        action.startPhase(JENKINS_JOB_SPAN_PHASE_QUEUE_WAITING_NAME, "first", null);
        // start a second phase without ending the first — simulates a missed onLeave callback
        action.startPhase(JENKINS_JOB_SPAN_PHASE_QUEUE_BLOCKED_NAME, "second", null);

        // The first phase must have been auto-closed
        var phases = action.getPhases();
        assertThat(phases.size(), is(1));
        assertThat(phases.get(0).phaseName(), is(JENKINS_JOB_SPAN_PHASE_QUEUE_WAITING_NAME));

        action.endCurrentPhase();
        assertThat(action.getPhases().size(), is(2));
        assertThat(action.getPhases().get(1).phaseName(), is(JENKINS_JOB_SPAN_PHASE_QUEUE_BLOCKED_NAME));
    }

    @Test
    void getPhases_returnsUnmodifiableLiveView() {
        var action = new QueueItemMonitoringAction();
        action.startPhase(JENKINS_JOB_SPAN_PHASE_QUEUE_WAITING_NAME, null, null);
        action.endCurrentPhase();

        List<QueuePhaseRecord> view = action.getPhases();
        assertThat(view.size(), is(1));

        // Adding more phases is visible through the live view — callers that need a snapshot
        // must copy the list themselves.
        action.startPhase(JENKINS_JOB_SPAN_PHASE_QUEUE_BUILDABLE_NAME, null, null);
        action.endCurrentPhase();
        assertThat(view.size(), is(2));

        // The view is unmodifiable — mutations must go through the action's own methods
        assertThrows(UnsupportedOperationException.class, () -> view.add(new QueuePhaseRecord("x", null, null, 0, 0)));
    }

    // ── concurrency ──────────────────────────────────────────────────────────

    @Test
    void concurrentStartAndEndDoNotCorruptState() throws InterruptedException {
        var action = new QueueItemMonitoringAction();
        int threads = 4;
        int iterationsPerThread = 50;
        var latch = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(threads);

        for (int i = 0; i < threads; i++) {
            final int id = i;
            executor.submit(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int j = 0; j < iterationsPerThread; j++) {
                    action.startPhase("phase-" + id, "reason", null);
                    action.endCurrentPhase();
                }
            });
        }

        latch.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

        // Every recorded phase must have a valid time range (no data corruption)
        for (var p : action.getPhases()) {
            assertTrue(p.endMillis() >= p.startMillis(), "phase end must be >= start: " + p.phaseName());
        }
    }
}
