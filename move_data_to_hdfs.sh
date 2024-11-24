if [ ! -d "data" ]; then
	./download_data
fi
hdfs dfs -put data/* data/
