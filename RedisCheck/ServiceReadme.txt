Redis Check installation as a service
---------------------------------------

Create Directory:
mkdir -p /home/RedisCheck

Copy Files:
copy redis_check.py to /home/RedisCheck
modify redis_check.py to use the correct smtp server (depending on dca location)
copy redischeck.service to /home/RedisCheck
copy /home/RedisCheck/redischeck.service to /etc/systemd/system/redischeck.service

Make Script Executable:
chmod +x /home/RedisCheck/redis_check.py

Set Permissions:
chmod 755 /home/RedisCheck
chmod 755 /home/RedisCheck/redis_check.py
chown root:root /home/RedisCheck/redis_check.py

chown root:root /etc/systemd/system/redischeck.service
chmod 644 /etc/systemd/system/redischeck.service

Start service:
systemctl daemon-reload
systemctl enable redischeck
systemctl start redischeck
systemctl status redischeck



NOTE: check the following and make sure it is correct
redis-cli -a 'PASSWORD' CONFIG GET dir
redis-cli -a 'PASSWORD' CONFIG GET dbfilename
