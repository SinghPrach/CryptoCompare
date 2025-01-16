# This will allow you to run a notebook from another Colab notebook
from google.colab import drive
from IPython import get_ipython

# Mount Google Drive to access notebooks
drive.mount('/content/drive')

# Execute another notebook by calling the URL
def run_notebook(notebook_path):
    # Use colab's `%run` magic to run a notebook.
    # Enclose the notebook path in quotes to handle spaces
    get_ipython().run_line_magic('run', f'"{notebook_path}"')  


# List the notebook URLs you want to run (Colab URLs or local Drive URLs)
# Note: For Google Drive, use the full path after mounting the drive
notebooks = [
    '/content/drive/My Drive/Colab Notebooks/Dim2_Top50Coins_CryptoCompare_via_API_In_USD.ipynb',
    '/content/drive/My Drive/Colab Notebooks/Fact_CryptoCompare_via_API_In_USD.ipynb',
    '/content/drive/My Drive/Colab Notebooks/AggFactDaily_CryptoCompare_via_API_In_USD.ipynb',
    '/content/drive/My Drive/Colab Notebooks/AggHistoricalData_CryptoCompare_via_API_In_USD.ipynb'
]

# Run each notebook in sequence
for notebook in notebooks:
    run_notebook(notebook)
