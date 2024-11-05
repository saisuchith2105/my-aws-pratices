#Write a Python program using Boto3 to list all EMR clusters in your AWS account and print their cluster IDs and statuses.


import boto3

def list_emr_clusters():
    # Create a Boto3 EMR client
    emr_client = boto3.client('emr')

    # List all clusters
    response = emr_client.list_clusters()

    # Check if there are clusters in the response
    if 'Clusters' in response:
        clusters = response['Clusters']
        
        # Print the cluster IDs and statuses
        print(f"{'Cluster ID':<20} {'Status':<20}")
        print("-" * 40)
        for cluster in clusters:
            cluster_id = cluster['Id']
            cluster_status = cluster['Status']['State']
            print(f"{cluster_id:<20} {cluster_status:<20}")
    else:
        print("No clusters found.")

if __name__ == "__main__":
    list_emr_clusters()
