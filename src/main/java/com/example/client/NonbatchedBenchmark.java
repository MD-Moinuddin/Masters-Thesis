package com.example.client;

/**
 * Entry point kept for the non-batched experiments. The client logic is identical to
 * {@link BatchedBenchmark}; "non-batched" is selected on the server side with
 * {@code TARA_BATCH_SIZE=1} when the Flink cluster is started.
 */
public class NonbatchedBenchmark {

    public static void main(String[] args) throws Exception {
        BatchedBenchmark.main(args);
    }
}
