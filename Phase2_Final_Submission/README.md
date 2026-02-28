# Multimodal RAG Pipeline with Snowflake Integration

## Deployment
Streamlit App:
https://lab4-app-mjyv2mwhbspeytdgemerrs.streamlit.app/

## Data Sources
- NOAA: https://www.nhc.noaa.gov/data/
- ECMWF: https://cds.climate.copernicus.eu
- CDC/ATSDR SVI: https://www.atsdr.cdc.gov/placeandhealth/svi/index.html
- UN OCHA HDX: https://data.humdata.org
- WorldPop: https://www.worldpop.org
- DeepMind / NVIDIA (Model references): https://deepmind.google

## Retrieval Methods
- Sparse: TF-IDF / BM25
- Dense: MiniLM + FAISS
- Hybrid Fusion
- Cross-Encoder Reranking

## Example Snowflake Queries
SELECT COUNT(*) FROM QUERY_LOGS;
SELECT * FROM WEATHER_ENSEMBLE LIMIT 5;

## Reproducibility
pip install -r requirements.txt
streamlit run lab4_app_main.py

Author: Adrija Ghosh
