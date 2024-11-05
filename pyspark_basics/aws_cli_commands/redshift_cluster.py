#Write a Python script to start a Redshift cluster if it is currently stopped.

import boto3
from botocore.exceptions import ClientError

# Initialize the boto3 Redshift client
redshift_client = boto3.client('redshift', region_name='your-region')  # Replace 'your-region' with the AWS region

# Specify your Redshift cluster identifier
cluster_identifier = 'your-cluster-identifier'  # Replace with your cluster identifier

try:
    # Check the current status of the Redshift cluster
    response = redshift_client.describe_clusters(ClusterIdentifier=cluster_identifier)
    cluster_status = response['Clusters'][0]['ClusterStatus']
    
    if cluster_status.lower() == 'stopped':
        # Start the cluster if it is currently stopped
        print(f"Cluster '{cluster_identifier}' is currently stopped. Starting the cluster...")
        redshift_client.start_cluster(ClusterIdentifier=cluster_identifier)
        print(f"Cluster '{cluster_identifier}' is now starting.")
    else:
        print(f"Cluster '{cluster_identifier}' is already {cluster_status}. No action needed.")
        
except ClientError as e:
    print(f"An error occurred: {e}")
