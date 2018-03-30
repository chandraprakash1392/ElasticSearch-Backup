#!/usr/bin/python

import json
import boto3
import urllib2
import requests
import time

def instance_start(instance, access_key, secret_token, region):
    ec2      = boto3.client('ec2',
          aws_access_key_id=access_key,
          aws_secret_access_key=secret_token,
          region_name=region,)

    response = ec2.start_instances(
          InstanceIds=instance,
          AdditionalInfo=Reserved)
   
    return response 


def es_get_indices(es_node):
    response  = urllib2.urlopen('http://%s/_cat/indices' %es_node).read()
    indices   = []

    for data in response.split('\n'):
        try:
            indices.append(data.split()[2])
        except IndexError:
            pass 

    return indices


def es_backup_remote(indices, es_node):
    # Cleaning up the existing indexes #
    for index in indices:
        requests.delete('http:127.0.0.1:9200/%s' %index)

    # Reindexing from the master ES #
    for index in indices:
        data = '{
            "source": {
                "remote": {
                "host": "http://%s:9200" %es_node,
                 },
                 "index": index,
              },
             "dest": {
                 "index": index,
              }
         }'
        response = requests.post('http://localhost:9200/_reindex', data=data)

def instance_stop(instance_id, access_key, secret_token, region):
    ec2      = boto3.client('ec2',
               aws_access_key_id=access_key,
               aws_secret_access_key=secret_token,
               region_name=region,)

    response = ec2.stop_instances(
               InstanceIds=instance)

    return response
 
if __name__ == "__main__":
   # Loading the parameters from Parameters.json file
   with open('parameters.json') as json_data:
        parameters     = json.load(json_data)

   instance_id         = parameters.get('instance_id')
   account             = parameters.get('account')
   access_key          = parameters.get('aws_access_key_id')
   secret_key          = parameters.get('aws_secret_key')
   region              = parameters.get('aws_region')
   remote_es_server    = parameters.get('master_es')

   status              = instance_start(instance_id, access_key, secret_key, region)
   time.sleep(90)
   if status.get('StartingInstances')[0].get('CurrentState').get('Code') == "0":
      indices          =  es_get_indices(remote_es_server)
      es_backup_remote(indices, remote_es_server) 

   status              = instance_stop(instance_id, access_key, secret_key, region) 
