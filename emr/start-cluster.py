import argparse
import datetime
import json
import logging
import os
import subprocess
import sys
import time
import yaml

def createInstanceGroup(config, groupType):
    configStr = 'InstanceGroupType=%s,InstanceType=%s' % (
        groupType.upper(), config['instance-type'])

    if not ('instance-count' in config) or groupType.upper() == 'MASTER':
        configStr = '%s,InstanceCount=1' % (configStr)
    else:
        configStr = '%s,InstanceCount=%d' % (
            configStr, config['instance-count'])

    if 'ebs' in config:
        configStr = '%s,EbsConfiguration={EbsOptimized=true,EbsBlockDeviceConfigs=[{VolumeSpecification={VolumeType=%s,SizeInGB=%d}}]}' % (
            configStr, config['ebs']['type'], config['ebs']['size'])

    return configStr

def appendProfileArgument(aws, cmd):
    if 'profile' in aws:
        return '%s --profile %s' % (cmd, aws['profile'])
    else:
        return cmd

def getClusterStartCommand(config):
    aws = config.get('aws')
    emr = aws['emr']
    ec2 = aws['ec2']

    arguments = '--name "%s" --release-label %s --applications %s' % \
        (emr['cluster-name'],
            emr['release-label'],
            ' '.join([('Name=%s' % app) for app in emr['applications']]))

    if 'default-roles' in emr and emr['default-roles']:
        arguments = '%s --use-default-roles' % (arguments)

    if 'tags' in aws:
        arguments = '%s --tags %s' % (arguments, ' '.join(
            [('%s=%s' % (tag, aws['tags'][tag])) for tag in aws['tags']]))

    # EC2 Attribs
    ec2Attribs = 'KeyName=%s' % ec2['key']

    if 'subnet' in ec2 and ec2['subnet'] is not None:
        ec2Attribs = '%s,SubnetId=%s' % (ec2Attribs, ec2['subnet'])
    if 'master-sg' in emr and 'slave-sg' in emr:
        ec2Attribs = '%s,EmrManagedMasterSecurityGroup=%s,EmrManagedSlaveSecurityGroup=%s' % (ec2Attribs, emr['master-sg'], emr['slave-sg'])

    arguments = '%s --ec2-attributes %s' % (arguments, ec2Attribs)

    nodes = config.get('nodes')
    instanceGroups = []
    if 'master' in nodes:
        # Instance Groups - Master
        instanceGroups.append(createInstanceGroup(nodes['master'], 'MASTER'))
    else:
        raise RuntimeError('No MASTER configuration specified')
    if 'core' in nodes:
        # Instance Gropus - CORE
        instanceGroups.append(createInstanceGroup(nodes['core'], 'CORE'))
    if 'task' in nodes:
        instanceGroups.append(createInstanceGroup(nodes['task'], 'TASK'))

    arguments = '%s --instance-groups %s' % (arguments, ' '.join(instanceGroups))

    arguments = appendProfileArgument(aws, arguments)

    return 'aws emr create-cluster %s' % arguments

def getClusterDetails(config, clusterId, startTime, logger):
    aws = config.get('aws')

    p = subprocess.Popen(appendProfileArgument(aws, 'aws emr list-instances --cluster-id %s --instance-group-types "MASTER"' % (clusterId)), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (output, err) = p.communicate()
    if output is not None:
        if type(output) is bytes:
            output = output.decode()
        if type(output) is str:
            output = json.loads(output)

            clusterInfo = {
                "clusterId": clusterId,
                "internalIp": output['Instances'][0]['PrivateIpAddress'],
                "externalIp": output['Instances'][0]['PublicIpAddress'],
                "creationDateTime": datetime.datetime.fromtimestamp(startTime).strftime('%Y-%m-%d %H:%M:%S')
            }

            logger.info('Current Cluster Details:')
            logger.info(clusterInfo)
            with open(config.get('currentCluster'), 'w') as currentCluster:
                json.dump(clusterInfo, currentCluster)

            historyFileExists = False
            if os.path.isfile(config.get('historyFile')):
                historyFileExists = True

            with open(config.get('historyFile'), 'a') as historyCluster:
                if (historyFileExists):
                    historyCluster.write(',\n')
                json.dump(clusterInfo, historyCluster)    

def terminateCluster(config, id, logger):
    aws = config.get('aws')
    cmd = 'aws emr terminate-clusters --cluster-ids %s' % (id)

    p = subprocess.Popen(appendProfileArgument(aws, cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (output, err) = p.communicate()

    output = output.decode()
    err = err.decode()
    
    if err is not None and err is not '':
        logger.error(err)
        logger.error('Could not call terminate on cluster %s, check if it is still running' % (id))
    if output is not None and output is '':
        logger.info('Successfully called terminate on cluster %s, it should terminate in a few minutes' % (id))

def waitForCluster(config, clusterId, logger):
    aws = config.get('aws')
    arguments = appendProfileArgument(aws, '')
    waiting = True

    while waiting:
        p = subprocess.Popen('aws emr describe-cluster %s --cluster-id %s' % (
            arguments, clusterId), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (output, err) = p.communicate()

        if output is not None:
            if type(output) is bytes:
                output = output.decode()
            if type(output) is str:
                output = json.loads(output)
                
                if output['Cluster']['Status']['State'] == 'WAITING':
                    logger.info(
                        'Cluster has successfully started, getting internal IP address')
                    startTime = output['Cluster']['Status']['Timeline']['CreationDateTime']
                    waiting = False

                    getClusterDetails(config, clusterId, startTime, logger)

                elif output['Cluster']['Status']['State'] == 'STARTING':
                    logger.info('Waiting for cluster to start')
                    time.sleep(10)
                else:
                    logger.error('Terminating Cluster - Unexpected Cluster Status - %s' % (output['Cluster']['Status']['State']))
                    terminateCluster(config, output['Cluster']['Id'], logger)
                    raise RuntimeError('Unexpected Cluster Status')
        else:
            if err is not None:
                raise RuntimeError(err)
            raise RuntimeError('Output is None')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Starts / Stops a configured EMR cluster according to a provided YAML config file')
    parser.add_argument('-c', '--config', type=str, required=True, help='Path to config.yaml file')
    parser.add_argument('-t', '--terminate', action='store_true', required=False, help='Terminates the cluster specifed by the currentCluster parameter in the config file or by the -i option')
    parser.add_argument('-i', '--id', type=str, required=False, help='Optional ID for the cluster to be terminated')
    parser.add_argument('-l', '--list', action='store_true', help='Lists active clusters')

    args = parser.parse_args()

    with open(args.config, 'r') as conf:
        config = yaml.load(conf)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    logger = logging.getLogger('emr-cluster-manager')
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)

    fh = logging.FileHandler(os.path.join(config.get(
        'log_dir'), 'emr-cluster-manager-%s.log' % time.strftime('%y%m%d-%H%M%S')))
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)

    logger.addHandler(ch)
    logger.addHandler(fh)

    aws = config.get('aws')

    if args.list:
        p = subprocess.Popen(appendProfileArgument(aws, 'aws emr list-clusters --active'), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (output, err) = p.communicate()
        
        if err is not None and type(err) is bytes:
            err = err.decode()
        
        if err is not None and err is not '':
            logger.error(err)
        elif output is not None:
            if type(output) is bytes:
                output = output.decode()
            if type(output) is str:
                logger.info(output)
    elif args.terminate:
        if args.id is not None:
            id = args.id
        else:
            with open(config.get('currentCluster'), 'r') as currentCluster:
                id = json.load(currentCluster)['clusterId']
        
        terminateCluster(config, id, logger)
    else:
        logger.info('Creating Cluster')

        p = subprocess.Popen(getClusterStartCommand(config),
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (output, err) = p.communicate()

        aws = config.get('aws')
        arguments = appendProfileArgument(aws, '')

        if err is not None:
            err = err.decode()
        
        if err is not '':
            logger.error(err)
        elif output is not None:
            if type(output) is bytes:
                output = output.decode()
            try:
                clusterId = json.loads(output)['ClusterId']
                logger.info('Created Cluster [%s]' % clusterId)
                waitForCluster(config, clusterId, logger)
            except json.decoder.JSONDecodeError:                
                logger.error('Return value was not valid JSON - [%s]' % output)
