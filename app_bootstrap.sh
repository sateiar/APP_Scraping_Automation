#
# Pre-install some packages in EMR for brq_time_series project
#
# Following instruction at https://stackoverflow.com/questions/31525012/how-to-bootstrap-installation-of-python-modules-on-amazon-emr
#
# sudo update-alternatives  --set python /usr/bin/python3.6

# sudo pip install -U python-geohash
sudo yum -y install python3-devel
sudo pip3 install -U pandas
sudo pip3 install -U requests==2.25.1
sudo pip3 install -U beautifulsoup4
sudo pip3 install -U fake-useragent
# sudo pip3 install -U urllib3==1.26.6
# sudo pip3 install -U tsfresh==0.11.2
# sudo pip3 install -U boto3
# sudo pip3 install pyarrow
# sudo pip3 install s3fs