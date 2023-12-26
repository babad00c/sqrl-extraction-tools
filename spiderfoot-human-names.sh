#!/bin/sh



echo $1
echo $2

python3 -c "import pandas as pd; df = pd.read_csv('$1'); unique_values = df['Data'].unique(); pd.DataFrame({'Name': unique_values, 'Organization': '$2'}).to_csv('$2-spiderfoot-names.csv', index=False)"