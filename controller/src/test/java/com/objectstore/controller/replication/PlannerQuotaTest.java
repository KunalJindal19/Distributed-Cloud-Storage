package com.objectstore.controller.replication;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

/** v5 §6.2: the planner's per-level quotas are floor plus largest remainder and add up exactly to the budget. */
class PlannerQuotaTest {

    @Test
    void theQuotasAddUpExactlyToTheBudgetAndFollowTheWeights() {
        for (int k = 1; k <= 10; k++) {
            for (long budget : new long[] {1, 2, 3, 7, 100, 1999, 2000, 2001}) {
                long[] quota = ReplicationPlanner.quotas(budget, k);
                assertThat(Arrays.stream(quota).sum()).as("k=%d budget=%d", k, budget).isEqualTo(budget);
                for (int i = 1; i < k; i++) {
                    assertThat(quota[i - 1]).as("a more urgent level never gets less").isGreaterThanOrEqualTo(quota[i]);
                }
            }
        }
        // v4 rounded each share up: 1143 + 572 + 286 = 2001 visits for a budget of 2000.
        assertThat(ReplicationPlanner.quotas(2000, 3)).containsExactly(1143, 571, 286);
    }
}
