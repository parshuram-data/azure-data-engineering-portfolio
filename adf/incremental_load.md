# Incremental Load Strategy

This document describes the incremental data loading approach used in the Azure Retail Data Platform.

## Objective

The pipeline processes only new or modified records instead of loading the complete source dataset during every pipeline execution.

This reduces data movement, processing time and unnecessary compute usage.

## Incremental Load Flow

```text
Source System
     ↓
Read Last Watermark
     ↓
Identify New/Modified Records
     ↓
ADF Copy Activity
     ↓
ADLS Gen2 Bronze
     ↓
Databricks Transformation
     ↓
Silver / Gold
     ↓
Update Watermark
