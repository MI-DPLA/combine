source activate combine
python /opt/combine/manage.py makemigrations
python /opt/combine/manage.py migrate
echo "from django.contrib.auth.models import User; User.objects.create_superuser('combine'"," 'root@none.com'"," 'combine')" | python /opt/combine/manage.py shell