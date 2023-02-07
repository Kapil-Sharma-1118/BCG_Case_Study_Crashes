spark-submit \
  --master local[*] \
  --deploy-mode client \
  --driver-memory 8g \
  --executor-memory 8g \
  --executor-cores 4  \
  Crashes.py