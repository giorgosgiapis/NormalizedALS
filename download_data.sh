wget https://files.grouplens.org/datasets/movielens/ml-latest-small.zip
unzip -o ml-latest-small.zip -d .
rm -rf data
mv ml-latest-small data
rm -f ml-latest-small.zip
