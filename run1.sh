echo "--------------------------------------------------------------------------"
echo "Before run, try to delete exist folder r1"
rm -Rf q1
echo "Clear!"
echo "--------------------------------------------------------------------------"
echo " "
echo "--------------------------------------------------------------------------"
echo "Run the file..."
echo "--------------------------------------------------------------------------"
pig -x local q1.pig
echo " "
echo "Finshed..."