package com.example;

public enum NodeType {
    // Sources
    REQUEST_SOURCE, GC_SOURCE, VIEW_SOURCE, RECORD_SOURCE,

    // Processors
    PROPOSER, COMMITTER, EXECUTOR, CONTROLLER,

    // Sinks
    RESULT_SINK, GC_SINK, VIEW_SINK, RECORD_SINK, REPLY_SINK,
    }
