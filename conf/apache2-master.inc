# to be included in /etc/apache2/sites-available/default
# make sure this is Include-d before apache2.inc.
# be sure to install libapache2-mod-wsgi package
WSGIDaemonProcess quartermaster processes=1 threads=5 display-name=%{GROUP} python-path=/opt/hq/lib
WSGIScriptAlias /hq/master /opt/hq/ws/master.py
#AliasMatch ^/hq/(.*\.(css|js|html|gif|png|jpg)) /opt/hq/static/$1
<Location /hq/master>
WSGIProcessGroup quartermaster
</Location>
<Directory /opt/hq/ws>
    Order allow,deny
    Allow from all
</Directory>
