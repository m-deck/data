#!/bin/bash

# Crawl the ECDC daily reports website and parse the latest report

BASE_DIR=`dirname "$0"`
python -m scrapy runspider "$BASE_DIR/crawl_ecdc_data.py" 2> /dev/null | \
    xargs wget -qq -O - | \
    python "$BASE_DIR/parse_ecdc_report_world.py"

# Run parser for Spain and Italy independently
python "$BASE_DIR/parse_dxy_api_country.py" "ES"
python "$BASE_DIR/parse_dxy_api_country.py" "IT"
