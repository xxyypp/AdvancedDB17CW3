echo "-------------------------------------"
echo "Start to copy file to /vol/automed/data/usgs"
cp /vol/automed/data/usgs/q0.pig
echo "-------------------------------------"
echo "Finished copied..."

echo " "
echo "-------------------------------------"
echo "Run the file..."
echo "-------------------------------------"
pig -x local q0.pig
echo " "
echo "Finshed..."