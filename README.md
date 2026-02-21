# Triennial AI Reporting & SQL Observability Platform

Enterprise-grade AI-powered reporting and SQL performance intelligence
platform deployed on **Databricks Compute Apps** using **Streamlit** and
LLM integration.

This system combines:

-   Research-grade structured report generation (NIH-style formatting)
-   UID-anchored citation intelligence
-   NLM-formatted PMID references
-   LLM-powered SQL diagnostics and recommendations
-   Dynamic performance observability dashboards
-   Production-ready DOCX export engine

------------------------------------------------------------------------

# Executive Summary

This project is designed as a modular AI-driven enterprise reporting and
analytics platform.

It serves two primary objectives:

1.  Generate publication-ready structured research reports with strict
    citation governance.
2.  Provide intelligent SQL Server performance diagnostics powered by
    LLM-based reasoning.

The platform emphasizes:

-   Citation accuracy
-   Hallucination control
-   Structured document integrity
-   Dynamic UI rendering
-   Production deployment inside Databricks

------------------------------------------------------------------------

# System Architecture

Databricks Compute App\
│\
├── Streamlit Frontend\
├── LLM Endpoint (Azure OpenAI / Custom Inference Endpoint)\
├── Delta Tables / SQL Performance Data\
├── UID-based Citation Engine\
├── NLM Reference Formatter\
└── DOCX Generator (Publication-Ready Output)

------------------------------------------------------------------------

# Module 1: Triennial Research Report Generator

## Purpose

Generate structured, publication-ready research reports with controlled
citation placement and reference formatting.

------------------------------------------------------------------------

## Core Capabilities

### 1. UID-Anchored Citation System

-   Citations are tied to UIDs (not paragraphs).
-   Citations are placed strictly at sentence boundaries.
-   Consecutive sentences from the same UID are grouped under a single
    citation number.
-   Prevents redundant superscript numbering.
-   No paragraph-end citation dumping.

### 2. Sentence-Level Citation Placement

The engine:

-   Detects sentence boundaries.
-   Validates UID-source mapping.
-   Prevents hallucinated citation numbers.
-   Ensures citations appear immediately after referenced statements.

### 3. NLM-Formatted PMIDs

References follow National Library of Medicine (NLM) style:

Author AB, Author CD. Title. Journal. Year;Volume(Issue):Pages. doi:xxx.
PMID: XXXXXXXX.

Not:

PMID XXXXX --- https://pubmed.ncbi.nlm.nih.gov/XXXXX/

### 4. Acronym Expansion Intelligence

If acronym expansion is not available:

-   Attempts dictionary-based lookup
-   Avoids printing "Expansion not specified"
-   Maintains clean academic formatting

### 5. Controlled Sections

-   Introduction and Summary limited to 2--3 sentences
-   No references inside summary
-   No entry numbers
-   No Pandoc exposure in preview
-   Section-level UID isolation enforced

------------------------------------------------------------------------

## Report Output

-   Clean Microsoft Word (.docx)
-   Structured headings
-   Superscript citations
-   Auto-generated footnotes
-   Fully formatted NLM reference list

------------------------------------------------------------------------

# Module 2: SQL Server Observability & Query Intelligence

## Purpose

Provide AI-powered diagnostics for SQL Server performance metrics using
structured data and LLM reasoning.

------------------------------------------------------------------------

## Data Sources

-   CPU utilization
-   Memory utilization
-   Wait types
-   Wait time (ms)
-   Most expensive query (CPU)
-   Most expensive query (physical reads)
-   Cache hit ratio
-   Windows events
-   Drill-down PowerBI URLs

------------------------------------------------------------------------

## UI Enhancements

### Dynamic Server Selector

-   Search field aligned left
-   Controlled dropdown width
-   Clean layout spacing
-   No full-width distortion

### Dynamic Wait Breakdown Table

-   Row count adapts to selected server
-   No static limitations
-   Fully data-driven rendering

### Most Expensive Queries Table

-   Fully dynamic rendering
-   Integrated recommendation engine
-   No separate recommendation tab

------------------------------------------------------------------------

# LLM-Powered Recommendation Engine

Hardcoded logic has been replaced with structured LLM generation.

Two interaction modes:

## 1. Generate Recommendations Button

User-triggered generation of:

-   Executive diagnosis
-   Query-level recommendations
-   Indexing strategy suggestions
-   CPU bottleneck analysis
-   Memory pressure signals
-   Wait type interpretation

## 2. Custom Query Intelligence Mode

Enables:

-   Prompt input field
-   Query-number referencing
-   Context-aware answers

Example:

Why is query #3 consuming high CPU?\
Is wait type SOS_WORK_DISPATCHER a critical issue?

------------------------------------------------------------------------

# LLM Integration

Supports:

-   Azure OpenAI
-   Custom inference endpoints
-   Enterprise LLM APIs

Prompt engineering ensures:

-   No markdown hash artifacts
-   No hallucinated section headers
-   Clean professional formatting
-   Structured diagnostic responses

------------------------------------------------------------------------

# Deployment Environment

-   Databricks Compute Apps
-   Python
-   Streamlit
-   Delta Tables
-   Azure AI (optional)

Example versioning:

APP VERSION: 2025-12-30 v13

------------------------------------------------------------------------

# Folder Structure

/ ├── app.py\
├── style_prompt.json\
├── requirements.txt\
├── README.md\
├── assets/\
├── vendor/\
└── data/

------------------------------------------------------------------------

# Technical Highlights

-   Sentence-aware citation injection
-   Consecutive citation grouping
-   UID isolation enforcement
-   Structured NLM formatting
-   Dynamic Streamlit UI alignment
-   LLM hallucination mitigation
-   Production-safe DOCX generation
-   No runtime binary downloads
-   Environment-based configuration

------------------------------------------------------------------------

# Security & Governance

-   No hardcoded API keys
-   Environment variable configuration
-   Structured LLM invocation
-   Controlled reference validation
-   Clean separation between UI and logic layers

------------------------------------------------------------------------

# Roadmap

-   Azure AI Hybrid Search integration
-   Semantic vector query exploration
-   Time-series anomaly detection for servers
-   GPU-accelerated inference pipelines
-   Kubernetes-based deployment model

------------------------------------------------------------------------

# Author

Habtamu Wolde\
Senior Data Systems Engineer\
AI-Driven Enterprise Analytics & Research Reporting Specialist
