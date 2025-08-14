# Abstract Data Pipeline (ADP)

Reusable, pluggable Python framework for fetching, cleaning, and exporting datasets.

## Diagram

```mermaid
flowchart LR
    A[Source] --> B[Transform(s)]
    B --> C[Sink]

    subgraph Pipeline
        direction LR
        A --> B --> C
    end
