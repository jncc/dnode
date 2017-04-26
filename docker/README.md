# Docker host setup
## Configure  to run as non root user

    sudo groupadd docker  
    sudo gpasswd -a ${USER} docker 
    sudo service docker restart 

log back in to apply
