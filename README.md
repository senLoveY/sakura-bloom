# 🌸 Sakura Bloom Analysis Project

This project investigates the phenology of cherry blossom flowering in Japan, with interconnected objectives:

* **Analyze long-term bloom trends** — Study how climate variability affects the timing of first and full bloom across Japan.
* **Assess tourism impact** — Explore how bloom timing influences visitor patterns and regional tourism dynamics.
* **Optimize travel routes** — Design an itinerary planner to help travelers experience multiple bloom events during a single trip.

The project combines historical phenological data, climate records, and geospatial analysis to deliver both scientific insights and practical tools.

## 📁 Repository Structure

`data/` — Raw and processed datasets

## 🌍 Datasets

The `data/` directory contains two primary CSV files:

first_bloom_with_coords.csv — Dates of first bloom recorded at observation sites.
full_bloom_with_coords.csv — Dates of full bloom recorded at observation sites.
 
## 🗂️ File Structure

Each file includes the following columns:

* **`Site Name`** — Name of the observation location (city or station)
* **`Currently Being Observed`** — Indicates whether the site is still active (`True`/`False`)
* **`1953–2025`** — Yearly columns containing actual bloom dates in ISO format (`YYYY-MM-DD 00:00:00`). Missing values indicate no observation for that year.
* **`30 Year Average 1991-2020`** — Climatological average date for the 1991–2020 reference period
* **`Notes`** — Additional metadata 
* **`latitude, longitude`** — Geographic coordinates of the observation site

 📚 Documentation & Coordination

Now all project documentation is maintained in Google Workspace:

* [Data Dictionary & Sources](https://docs.google.com/spreadsheets/d/1zAyRRtdj583dBfJ4CoIZ0DjpiC2ijnxafgmcf1yp35I/edit?usp=sharing) –
Descriptions of tables, data provenance, licenses, and collection methodology.
* [Data Engineering Tasks](https://docs.google.com/document/d/15Ay1zvp3ntc2b7aK3LPay7PzhS-9-kGoGz4osB9T7JU/edit?usp=sharing) –
Task board for data engineers: assignments, deadlines, and progress tracking.
* [Kaggle Datasets](https://www.kaggle.com/datasets/ryanglasnapp/japanese-cherry-blossom-data) –
Publicly available cherry blossom datasets used as primary sources.


