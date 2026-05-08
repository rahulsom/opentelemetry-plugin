/*
 * Copyright The Original Author or Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.jenkins.plugins.opentelemetry.queue;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Immutable record of a single completed queue state phase for a build.
 *
 * <p>Phases are appended in chronological order, so {@code phases.get(0).startMillis()} is always
 * the earliest timestamp.
 */
public record QueuePhaseRecord(
        @NonNull String phaseName,
        @Nullable String reason,
        @Nullable String label,
        long startMillis,
        long endMillis) {}
