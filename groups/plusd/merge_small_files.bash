#!/bin/bash
# merge script

# Creates larger html files from small gzipped html files

# Function merges temp files
function create_big_file { 
	echo "<nb_documents>$1</nb_documents>" >> "files_merged/$3_to_$4.html";
	cat .tmp_index_big_file$2 .tmp_big_file$2 >> "files_merged/$3_to_$4.html";
	rm .tmp_index_big_file$2* .tmp_big_file$2*
	echo "Created files_merged/$3_to_$4.html"
}

# Creates a temp file with the index of the new file
function create_file_index {
	echo "<document_id id=$1/>" >> .tmp_index_big_file$2;
}

# Creates a temp file with the content of the new file
function create_file_content {
	echo "<document_content id=$1>" >> .tmp_big_file$i
	zcat "$f" >> .tmp_big_file$i;
	echo "</document_content>" >> .tmp_big_file$i
	mv "$f" "files_merged/saved/"
}

# Maximum number of documents to merge
if [ -z "$1" ]
  then
    max_num_documents=1000
  else
  	max_num_documents=$1
fi

# Approximative size of the larger document
if [ -z "$2" ]
  then
    max_size=1000000
  else
  	max_size=$2
fi

# Variables init
num_documents=0; #current number of documents treated
i=0; # index of the larger files
mkdir -p "files_merged/saved/"
j=0; # Counts the number of files in a large file
filesize=0;
first_doc="";
current_doc="";

shopt -s nullglob
for f in *.gz
do
	let "j=(j+1)"
	let "num_documents=(num_documents+1)"

	fs=$(stat -c%s "$f")
	let "filesize=fs+filesize"
	filename=$(basename "$f" .gz)

	if [ $j -eq 1 ]; then first_doc=$filename; 
	fi

	current_doc=$filename
	
	create_file_index $filename $i

	create_file_content $filename $f

	if [ $num_documents -gt $max_num_documents ]; then
		break;
	fi

	if [ $filesize -gt $max_size ]; then

		create_big_file $j $i $first_doc $current_doc

		let "i=(i+1)"
		let "j=0"
		let "filesize=0"
	fi
done

if [ $filesize -gt 0 ]; then
	create_big_file $j $i $first_doc $current_doc
fi

#for f in *.html.gz; 
#do
#	let "nbfiles=(nbfiles+1)%2"

#	if [ $nbfiles -lt 2 ]; then
#		zcat "$f" >> big_file$nbfiles.html;
#	fi
#done