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
# Author

Habtamu Wolde\
Senior Data Systems Engineer\
AI-Driven Enterprise Analytics & Research Reporting Specialist
