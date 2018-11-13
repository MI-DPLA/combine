# script to update Combine
if [ $# -eq 1 ]
	then			
		RELEASE=$1
		/usr/local/anaconda/envs/combine/bin/python manage.py update --release $RELEASE
	else
		/usr/local/anaconda/envs/combine/bin/python manage.py update
fi