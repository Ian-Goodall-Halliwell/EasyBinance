EasyBinance
===========

EasyBinance is a Python library designed to simplify the process of downloading historical financial data from the Binance API. The library provides an easy-to-use interface for fetching candlestick (Kline) data and organizing it for further analysis. Whether you're a quantitative analyst, data scientist, or cryptocurrency enthusiast, EasyBinance streamlines the retrieval and management of Binance data for your projects.

Features
--------

-   Simplified Data Download: Easily download historical Kline data from Binance with customizable parameters.
-   Parallel Processing: Leverage parallel processing to enhance download efficiency, especially for a large number of tokens.
-   Append to Existing Data: Seamlessly append new data to existing CSV files, ensuring your dataset is up-to-date.
-   Customizable: Tailor the library to your needs by adjusting parameters such as time intervals, market capitalization thresholds, and tokens.

Installation
------------

Before using EasyBinance, ensure you have the required dependencies installed:



```
pip install pandas python-binance joblib
```

Getting Started
---------------

1.  API Keys: Obtain Binance API keys and store them in a CSV file (e.g., `clients.csv`).

    `
    YOUR_API_KEY_1, YOUR_API_SECRET_1`

    `
    YOUR_API_KEY_2, YOUR_API_SECRET_2`

    `...`

2.  Run the Example Script:

    Execute the `example.py` script to download historical data from Binance.

    

    ```
    python example.py
    ```

3.  Customization:

    Customize the script parameters in `example.py` according to your requirements.

Example Usage
-------------

pythonCopy code

```python
from EasyBinance.pull_data import download

out = download(
    clients="C:/Users/Ian/Desktop/clients.csv",
    datapath="data",
    starttime="2023-01-01 00:00:00",
    endtime="now",
    freq="5m",
    mcap="5000k",
    tokens=["BUSD"]
)
```

File Structure
--------------

### 1\. `example.py`

This file demonstrates the usage of the EasyBinance library. Customize the parameters to fit your specific needs.

### 2\. `pull_data.py`

Contains the core functionality of the EasyBinance library, including functions for Binance API calls, data retrieval, and data download.

Notes
-----

-   The script will create and use the `data/raw` directory for storing initial data and `data/app` for appended data.
-   Ensure API keys have the necessary permissions for fetching historical Kline data.
-   The library supports various time intervals, market capitalization thresholds, and token filters.

License
-------

This project is licensed under the MIT License - see the license file for details.

Feel free to explore and modify the EasyBinance library for your specific use case. Contributions are welcome!