# to be included in /etc/apache2/sites-available/default
# be sure to install libapache2-mod-wsgi package
WSGIDaemonProcess headquarters processes=1 threads=12 display-name=%{GROUP} python-path=/opt/hq/lib/python2.7/site-packages
WSGIDaemonProcess hqstatus processes=1 threads=5 display-name=%{GROUP} python-path=/opt/hq/lib/python2.7/site-packages
WSGIDaemonProcess hqinq processes=5 threads=1 display-name=%{GROUP} python-path=/opt/hq/lib/python2.7/site-packages
WSGIScriptAlias /hq/jobs /opt/hq/ws/hq.py
WSGIScriptAlias /hq/inq /opt/hq/ws/inq.py
WSGIScriptAlias /hq /opt/hq/ws/status.py
AliasMatch ^/hq/(.*\.(css|js|html|gif|png|jpg)) /opt/hq/static/$1

RewriteEngine On
RewriteRule ^/hq/jobs/([^/]+/(m?discovered|testinq)) /hq/inq/$1 [PT,L]

<Location /hq>
WSGIProcessGroup hqstatus
</Location>
<Location /hq/jobs>
WSGIProcessGroup headquarters
</Location>
<IfModule mod_deflate.c>
AddOutputFilterByType DEFLATE text/json
</IfModule>
<Location /hq/inq>
WSGIProcessGroup hqinq
</Location>
<Directory /opt/hq/ws>
    Order allow,deny
    Allow from all
</Directory>
