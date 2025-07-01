# data-check


Data-Check allows you to compare 2 tables / views or SQL queries in BigQuery and nicely output differences for you to review data changes and share these insights.

For those who were using Data-Diff from Datafold, it's a similar tool 😉

## Installation

```bash
# Create python env (i.e. 3.11)

pip install -r requirements.txt
pip install .
streamlit run data_check/streamlit_app.py
```