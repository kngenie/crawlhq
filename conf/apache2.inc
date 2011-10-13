# to be included in /etc/apache2/sites-available/default
# be sure to install libapache2-mod-wsgi package
WSGIDaemonProcess headquarters processes=1 threads=40 display-name=%{GROUP} python-path=/opt/hq/lib
WSGIDaemonProcess hqstatus processes=1 threads=5 display-name=%{GROUP} python-path=/opt/hq/lib
WSGIScriptAlias /hq/status /opt/hq/ws/status.py
WSGIScriptAlias /hq/jobs /opt/hq/ws/hq.py
AliasMatch ^/hq/(.*\.(css|js|html|gif|png|jpg)) /opt/hq/static/$1
<Location /hq/jobs>
WSGIProcessGroup headquarters
</Location>
<IfModule mod_deflate.c>
AddOutputFilterByType DEFLATE text/json
</IfModule>
<Location /hq/status>
WSGIProcessGroup hqstatus
</Location>
<Directory /opt/hq/ws>
    Order allow,deny
    Allow from all
</Directory>
