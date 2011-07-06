# to be included in /etc/apache2/sites-available/default
# be sure to install libapache2-mod-wsgi package
WSGIDaemonProcess headquarters processes=1 threads=40 display-name=%{GROUP} python-path=/opt/hq/lib
WSGIProcessGroup headquarters
WSGIScriptAlias /hq/status /opt/hq/ws/status.py
WSGIScriptAlias /hq/jobs /opt/hq/ws/hq.py
AliasMatch ^/hq/(.*\.(css|js|html|gif|png|jpg)) /opt/hq/static/$1
<Directory /opt/hq/ws>
    Order allow,deny
    Allow from all
</Directory>
