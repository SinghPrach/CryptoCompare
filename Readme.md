- I created an account on CryptoCompare website to obtain the API key.
- Since I am using the version, I have access to only 11,000 API calls monthly.
- This is why, I have tested this pipeline for 7 days and restricted the analysis for only 50 cryptos. [Reason: For free-tier users, CryptoCompare allows only symbols in a single request.]
- Everyday, data of top 50 cryptos are stored in the database, based on isTrading=active parameter and sorted based on highest TotalCoinsMined.
- Some other attributes based on which sorting can happen are alphabetical order of symbols, coinName, Algorithm, Id, Categories, CoinImageUrl.
- Implemented the solution by calling RESTful API

- **Storage** -  Google Drive
- **Notebook** - Google Colab
- **Compute Engine** - Python 3 Google Compute Engine

- Tables information:
  1. 1 Fact Table - Information on all fact data
  2. 2 Dimension Tables - Information on crypto symbols and names to map the names of crypto to Fact data using symbols as joining keys and the top 50 coins based on TotalCoinsMined parameter.
  3. 1 Aggregated Table - Obtained after joining the Dimension and Fact tables, and then aggregating.
- Usual time of run - 11:00 AM
- Dates of run - 13th Jan 2025, 15th Jan 2025, 16th Jan 2025, 20th Jan 2025, 21st Jan 2025 [Gap in run days' continuity due to personal reasons]
- Flowchart of Data Engineering Pipeline:

![image](https://github.com/user-attachments/assets/c724536a-3107-497e-80cc-17b830273e0d)

- Run Flow:
- Call the notebook "**ExecutePipeline.py** directly, it will automate running all the notebooks in the required sequence.
<img width="1415" alt="Screenshot 2025-01-16 at 11 44 25" src="https://github.com/user-attachments/assets/a3dd6638-a8ad-4cde-a4bd-0deb3b8cdcb2" />

Notebooks' run order:
1. Dim1_ListOfNamesCrypto_CryptoCompare_via_API_In_USD.py (Monthly once, 1st of every month)
2. Dim2_Top50Coins_CryptoCompare_via_API_In_USD.py (Daily)
3. Fact_CryptoCompare_via_API_In_USD.py (Daily)
4. AggFactDaily_CryptoCompare_via_API_In_USD.py (Daily)
5. AggHistoricalData_CryptoCompare_via_API_In_USD.py (Daily)
