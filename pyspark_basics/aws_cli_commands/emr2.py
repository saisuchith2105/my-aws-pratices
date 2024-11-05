# Write a Python script to start a new EMR cluster with a specific configuration and print the cluster ID.

import boto3
import time

def create_emr_cluster():
    emr_client = boto3.client('emr', region_name='us-west-2')  # Change the region as needed

    # Specify the cluster configuration
    cluster_response = emr_client.run_job_flow(
        Name='MyEMRCluster',
        ReleaseLabel='emr-6.10.0',  # Specify the EMR release version
        Instances={
            'MasterInstanceType': 'm5.xlarge',
            'SlaveInstanceType': 'm5.xlarge',
            'InstanceCount': 3,  # 1 master and 2 core nodes
            'KeepJobFlowAliveWhenNoSteps': True,
        },
        Steps=[
            {
                'Name': 'Install Application',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['hadoop-streaming', '-input', 's3://your-input-bucket', '-output', 's3://your-output-bucket', '-mapper', 'your-mapper.py', '-reducer', 'your-reducer.py']
                }
            }
        ],
        JobFlowRole='EMR_EC2_DefaultRole',  # Ensure this role exists
        ServiceRole='EMR_DefaultRole',  # Ensure this role exists
        VisibleToAllUsers=True,
    )

    # Get the cluster ID
    cluster_id = cluster_response['JobFlowId']
    print(f"Cluster created with ID: {cluster_id}")

    return cluster_id

if __name__ == "__main__":
    create_emr_cluster()

 
 