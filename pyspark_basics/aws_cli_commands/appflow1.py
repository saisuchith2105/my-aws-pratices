#Write a Python script using Boto3 to list all AppFlow flows in your AWS account.

import boto3

def list_appflow_flows():
    # Initialize the Boto3 client for AppFlow
    client = boto3.client('appflow')
    
    # List all AppFlow flows
    try:
        response = client.list_flows()
        flows = response.get('flows', [])

        if flows:
            print("AppFlow Flows in your account:")
            for flow in flows:
                print(f"Flow Name: {flow['flowName']}, Flow Status: {flow['flowStatus']}")
        else:
            print("No AppFlow flows found in your account.")
    
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    list_appflow_flows()
