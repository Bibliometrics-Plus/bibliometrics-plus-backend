# Bibliometrics+

Bibliometrics+ is a library analytics dashboard project focused on comparing public library data across cities and surfacing KPI and EDI-related insights. The project currently uses Ottawa, Toronto and Montreal data in Supabase.

## Project Purpose

The goal of Bibliometrics+ is to turn open public library datasets into a dashboard that supports:

- branch and system-level KPI analysis
- EDI-aware contextual analysis
- cross-city comparison
- future AI-generated insights and recommendations

The project combines ETL scripts, a Supabase PostgreSQL backend, and a Streamlit dashboard.

## Current Features

### Backend / Data
- initial ETL and DB loader scripts for Ottawa datasets
- Toronto ETL scripts merged into the shared backend
- Ottawa census context loaded into Supabase for EDI analysis
- branch-to-ward mapping for Ottawa
- Supabase views for Ottawa EDI prioritization

### Dashboard
- Home page
- Data Status page
- KPI Dashboard
- EDI Analytics page
- AI Insights page

### Ottawa EDI Analytics
The Ottawa EDI workflow currently includes:
- Ottawa branch to ward mapping
- Ottawa ward-level census context
- filtered EDI indicators
- branch-level Ottawa EDI priority ranking

The current Ottawa EDI score is a weighted equity-context prioritization model based on:
- core housing need
- age 0 to 14
- age 65 plus
- immigrant population

This score is intended as an initial prioritization signal, not yet a full service-gap model.

## Tech Stack

- Python
- Streamlit
- Supabase / PostgreSQL
- SQLAlchemy
- Pandas
- Altair
- GitHub
- Jira

## Repository

GitHub repository: `https://github.com/Bibliometrics-Plus/bibliometrics-plus-backend`

Active integration branch: `dev`

## Repository Structure

```text
backend/
├── app.py
├── config.py
├── db.py
├── check_env.py
├── check_env_only.py
├── check_data_files.py
├── load_libraries_from_locations.py
├── load_circulation_from_loans.py
├── load_collection_items_from_most_requested.py
├── load_user_group_stats_from_cardholders.py
├── load_ottawa_census_context.py
├── raw/
├── app/
│   ├── pages/
│   └── services/
└── .streamlit/
    └── secrets.toml
