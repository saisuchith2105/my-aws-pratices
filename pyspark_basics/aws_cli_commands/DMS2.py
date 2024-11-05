import boto3

def start_dms_task(task_arn):
    # Initialize a DMS client
    client = boto3.client('dms')
    
    try:
        # Start the replication task
        response = client.start_replication_task(
            ReplicationTaskArn=task_arn,
            StartReplicationTaskType='start-replication'
        )
        print("Replication task started successfully:", response)
    except client.exceptions.ResourceNotFoundFault:
        print(f"Task with ARN {task_arn} not found.")
    except client.exceptions.InvalidResourceStateFault:
        print(f"Task with ARN {task_arn} is in an invalid state to start.")
    except Exception as e:
        print("Error starting replication task:", e)

# Replace 'your-task-arn' with the actual task ARN
task_arn = 'your-task-arn'
start_dms_task(task_arn)
