package io.agentops.lite.core.domain;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Locks the canary algorithm to fixed cross-implementation examples. */
class CanaryBucketTest {
  @Test
  void returnsStableUnsignedBuckets() {
    assertThat(CanaryBucket.calculate("user-001", "release-001")).isEqualTo(2);
    assertThat(CanaryBucket.calculate("user-002", "release-001")).isEqualTo(59);
    assertThat(CanaryBucket.calculate("user-001", "release-002")).isEqualTo(10);
  }
}
