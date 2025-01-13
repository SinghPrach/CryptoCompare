- I created an account on CryptoCompare website to obtain the API key.
- Since I am using the version, I have access to only 11,000 API calls monthly.
- This is why, I have tested this pipeline for 7 days and restricted the analysis for only 14 cryptos.
- Implemented the solution by calling RESTful API

- **Storage** -  Google Drive
- **Notebook** - Google Colab
- **Compute Engine** - Python 3 Google Compute Engine

- Tables information:
  1. 1 Fact Table - Information on all fact data
  2. 1 Dimension Table - Information on crypto symbols and names to map the names of crypto to Fact data using symbols as joining keys.
  3. 1 Aggregated Table - Obtained after joining the Dimension and Fact tables, and then aggregating.
- Usual time of run - 11:00 AM
- Dates of run - 13th Jan 2025, 14th Jan 2025, 15th Jan 2025, 16th Jan 2025, 20th Jan 2025, 21st Jan 2025 [Gap in run days' continuity due to personal reasons]
