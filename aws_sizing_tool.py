#!/usr/bin/env python3
"""
Eon Customer Environment Sizing Tool

This script helps customers size their current backup spend by collecting
information about AWS resources that are applicable for backup.
"""

import argparse
import concurrent.futures
import csv
import datetime
import os
import signal
import sys
import json
import atexit
import threading
import uuid
import gzip

# Check for required dependencies
try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
except ImportError:
    print("\nERROR: Missing required AWS SDK packages. Run the following command to install:\n")
    print("pip install --upgrade boto3 botocore")
    sys.exit(1)

# Version information
TOOL_VERSION = "2026.02.28-ee02ccb"

# Global configurations
OUTPUT_DIR = 'eon_sizing_results'
INVENTORY_FILE = 'inventory.csv'
SNAPSHOTS_FILE = 'snapshots.csv'
SUMMARY_FILE = 'summary.txt'
ERROR_LOG_FILE = 'errors.log'
SECONDS_PER_DAY = 86400 # 24 * 60 * 60

# Global for logging AWS requests
AWS_REQUEST_LOG_FILE = 'aws_requests.log.gz'
aws_log_file_handle = None
thread_local_data = threading.local() # For request correlation

# Global data storage
inventory_data = []
snapshot_data = []
errors_log = []
backup_data = []  # Store AWS Backup plan and selection data
account_alias_map = {}  # Cache of AWS Account ID -> Account Alias

# Global S3 bucket executor shared across accounts
s3_bucket_executor = None

def _shutdown_s3_executor():
    global s3_bucket_executor
    try:
        if s3_bucket_executor is not None:
            s3_bucket_executor.shutdown(wait=True)
    except Exception:
        pass
backup_recovery_points = []  # Store AWS Backup recovery points data

# AWS Backup account tracking
backup_accounts_with_config = set()  # Accounts that have AWS Backup configured
backup_accounts_with_errors = set()  # Accounts that had errors retrieving AWS Backup data

# Resource summary counters
summary = {
    'EC2': {'count': 0, 'storage': 0, 'snapshots': 0, 'backup_storage': 0},
    'RDS': {'count': 0, 'storage': 0, 'snapshots': 0, 'backup_storage': 0},
    'DynamoDB': {'count': 0, 'storage': 0, 'snapshots': 0, 'backup_storage': 0},
    'S3': {'count': 0, 'objects': 0, 'storage': 0, 'backup_storage': 0, 'inventory_enabled_count': 0, 'total_inventory_configs': 0},
    'EKS': {'count': 0, 'volumes': 0, 'storage': 0, 'backup_storage': 0},
    'EFS': {'count': 0, 'storage': 0, 'snapshots': 0, 'backup_storage': 0},
    # Redshift storage fields explanation:
    # - 'storage': Calculated USED storage based on cluster capacity and utilization percentage  
    # - 'total_provisioned_storage_gb': Total PROVISIONED storage capacity across all clusters
    # - 'total_backup_size_gb': Native Redshift backup storage (snapshots, automated backups, etc.)
    # - 'backup_storage': AWS Backup service storage (separate from native backups)
    'Redshift': {'count': 0, 'storage': 0, 'snapshots': 0, 'total_backup_size_gb': 0, 'total_provisioned_storage_gb': 0, 'backup_storage': 0},
    'DocumentDB': {'count': 0, 'storage': 0, 'snapshots': 0, 'backup_storage': 0},
    'OpenSearch': {'count': 0, 'storage': 0, 'snapshots': 0, 'backup_storage': 0},
    'FSx': {'count': 0, 'storage': 0, 'snapshots': 0, 'backup_storage': 0},
    'AWS_Backup': {'configured': False},
    'Accounts': {'count': 0}
}

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Eon Customer Environment Sizing Tool')
    
    parser.add_argument(
        '--all', 
        action='store_true',
        dest='all_accounts',
        help='Count resources in all accounts in the current AWS Organization',
        default=False
    )
    
    parser.add_argument(
        '--id',
        dest='account_id',
        help='Count resources in the specified AWS Account ID',
        default=None
    )
    
    parser.add_argument(
        '--accounts-file',
        dest='accounts_file',
        help='Count resources in accounts listed in a file (one ID per line)',
        default='accounts.txt'
    )
    
    parser.add_argument(
        '--ou',
        dest='ou_id',
        help='Count resources in all accounts under the specified Organizational Unit ID',
        default=None
    )
    
    parser.add_argument(
        '--role-name',
        dest='role_name',
        help='IAM role name to use when assuming access to other accounts',
        default='OrganizationAccountAccessRole'
    )
    
    # Add mutually exclusive group for cloud partitions
    partition_group = parser.add_mutually_exclusive_group()
    partition_group.add_argument(
        '--gov',
        action='store_true',
        dest='use_gov',
        help='Use AWS GovCloud regions and ARN format',
        default=False
    )
    partition_group.add_argument(
        '--china',
        action='store_true',
        dest='use_china',
        help='Use AWS China regions and ARN format',
        default=False
    )
    
    parser.add_argument(
        '--max-workers',
        dest='max_workers',
        help='Maximum parallel processing requests',
        type=int,
        default=min(32, (os.cpu_count() or 1) + 4)
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        dest='verbose_mode',
        help='Output verbose debugging information',
        default=False
    )
    
    return parser.parse_args()

def signal_handler(sig, frame):
    """Handle Ctrl+C signal"""
    print("\nExiting...")
    sys.exit(0)

def close_aws_log_file():
    """Closes the AWS log file if it's open."""
    global aws_log_file_handle
    if aws_log_file_handle:
        try:
            aws_log_file_handle.close()
            aws_log_file_handle = None
        except Exception as e:
            # Avoid crashing during exit if close fails
            print(f"\nWARNING: Error closing AWS log file: {e}\n")

# Helper function to create the base log entry dictionary
def _create_base_log_entry(event_type, request_id, kwargs):
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    event_name_parts = kwargs.get('event_name', '').split('.')
    service_name = event_name_parts[1] if len(event_name_parts) > 1 else 'unknown-service'
    operation_name = event_name_parts[2] if len(event_name_parts) > 2 else 'unknown-operation'

    return {
        'timestamp': timestamp,
        'type': event_type,
        'request_id': request_id,
        'service': service_name,
        'operation': operation_name,
    }

def log_aws_request(params, **kwargs):
    """Logs AWS API request details."""
    global aws_log_file_handle
    if not aws_log_file_handle:
        return

    request_id = None # Initialize request_id
    try:
        request_id = str(uuid.uuid4())
        thread_local_data.current_request_id = request_id
        log_entry = _create_base_log_entry('REQUEST', request_id, kwargs)
        log_entry['params'] = params

        aws_log_file_handle.write(json.dumps(log_entry, default=str) + '\n')
    except Exception as e:
        try:
            timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat() # Get timestamp again for fallback
            aws_log_file_handle.write(f'{{"timestamp": "{timestamp}", "type": "LOGGING_ERROR", "request_id": "{request_id}", "stage": "REQUEST", "error": "{str(e)}"}}')
        except: # Ignore errors during fallback write
            pass

def log_aws_response(parsed, http_response=None, model=None, **kwargs):
    """Logs AWS API response details."""
    global aws_log_file_handle
    if not aws_log_file_handle:
        return

    request_id = None # Initialize request_id
    try:
        request_id = getattr(thread_local_data, 'current_request_id', None)
        log_entry = _create_base_log_entry('RESPONSE', request_id, kwargs)
        log_entry['status_code'] = http_response.status_code if http_response else 'N/A'
        log_entry['response'] = parsed

        aws_log_file_handle.write(json.dumps(log_entry, default=str) + '\n')
    except Exception as e:
         try:
            timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat() # Get timestamp again for fallback
            aws_log_file_handle.write(f'{{"timestamp": "{timestamp}", "type": "LOGGING_ERROR", "request_id": "{request_id}", "stage": "RESPONSE", "error": "{str(e)}"}}')
         except: # Ignore errors during fallback write
            pass

def setup_output_directory():
    """Create output directory with timestamp"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"{OUTPUT_DIR}_{timestamp}"
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    global aws_log_file_handle
    log_file_path = os.path.join(output_dir, AWS_REQUEST_LOG_FILE)
    try:
        aws_log_file_handle = gzip.open(log_file_path, 'wt', encoding='utf-8')
        atexit.register(close_aws_log_file)
        verbose_print(f"Raw AWS API calls will be logged to: {log_file_path}")
    except Exception as e:
        error_print(f"\nWARNING: Could not open {log_file_path} for writing. AWS requests will not be logged. Error: {e}\n")
        aws_log_file_handle = None

    return output_dir

def verbose_print(message):
    """Print verbose debug messages"""
    if args.verbose_mode:
        print(f"DEBUG: {message}")

def error_print(details, account='', print_to_console=False):
    """Log errors"""
    account_info = f"Account: {account} " if account else ""
    error_msg = f"ERROR: {account_info}{details}"
    print(f"\n{error_msg}\n") if print_to_console else verbose_print(f"\n{error_msg}\n")
    errors_log.append(error_msg)

def bytes_to_gb(num_bytes):
    return num_bytes / (1024 * 1024 * 1024)

def format_size_gb(size_gb):
    """Format size in GB to avoid scientific notation and set small values to 0"""
    # Treat anything less than 1MB (0.001 GB) as 0
    if size_gb < 0.001:
        return "0"
    
    # Format with 3 decimal places but remove trailing zeros
    return f"{size_gb:.3f}".rstrip('0').rstrip('.')

def add_to_inventory(account_id, resource_type, resource_id, source_size, region,
                    snapshot_count=0, first_snapshot_date=None, latest_snapshot_date=None, tags=None, object_count=None, s3_inventory_configs=None):
    """Add resource to inventory data"""
    inventory_data.append({
        'account_id': account_id,
        'resource_type': resource_type,
        'resource_id': resource_id,
        'region': region,
        'source_size': source_size,
        'snapshot_count': snapshot_count,
        'first_snapshot_date': first_snapshot_date,
        'latest_snapshot_date': latest_snapshot_date,
        'tags': tags or {},
        'object_count': object_count,
        's3_inventory_configs': s3_inventory_configs
    })

def add_to_snapshots(account_id, resource_type, snapshot_id, snapshot_date, snapshot_size, region, source_resource_id=None, tags=None, full_snapshot_size_bytes=None):
    """Add snapshot to snapshot data"""
    snapshot_data.append({
        'account_id': account_id,
        'resource_type': resource_type,
        'snapshot_id': snapshot_id,
        'region': region,
        'snapshot_date': snapshot_date,
        'snapshot_size': snapshot_size,
        'source_resource_id': source_resource_id, # Add source resource ID
        'tags': tags or {},
        'full_snapshot_size_bytes': full_snapshot_size_bytes
    })

def get_current_account():
    """Get the current AWS account details"""
    try:
        client = boto3.client('sts')
        response = client.get_caller_identity()
        return {
            'Account': response['Account'],
            'Arn': response['Arn']
        }
    except Exception as e:
        error_print(f"Failed to get current account details: {str(e)}")
        sys.exit(1)

def get_organization_accounts():
    """Get all accounts in the AWS Organization"""
    accounts = []
    root_account_id = None
    
    try:
        client = boto3.client('organizations')
        paginator = client.get_paginator('list_accounts')
        
        for page in paginator.paginate():
            for account in page['Accounts']:
                if account['Status'] == 'ACTIVE':
                    accounts.append({
                        'Id': account['Id'],
                        'Name': account['Name']
                    })
                    
                    # Check if it's the management account
                    if 'ManagementAccountId' in account:
                        root_account_id = account['ManagementAccountId']
        
        if not root_account_id and accounts:
            # Try to determine the management account
            response = client.describe_organization()
            if 'Organization' in response and 'MasterAccountId' in response['Organization']:
                root_account_id = response['Organization']['MasterAccountId']
            
    except Exception as e:
        error_print(f"Failed to get organization accounts: {str(e)}")
    
    return root_account_id, accounts

def get_ou_accounts(ou_id):
    """Get all accounts in an Organizational Unit and its child OUs"""
    accounts = []
    root_account_id = None
    
    try:
        client = boto3.client('organizations')
        
        # First, get the management account ID
        try:
            response = client.describe_organization()
            if 'Organization' in response:
                if 'MasterAccountId' in response['Organization']:
                    root_account_id = response['Organization']['MasterAccountId']
                elif 'ManagementAccountId' in response['Organization']:
                    root_account_id = response['Organization']['ManagementAccountId']
        except Exception as org_e:
            error_print(f"Error getting organization details: {str(org_e)}")
        
        # Helper function to recursively get accounts in an OU and its children
        def get_accounts_in_ou(ou_id):
            # Get accounts directly in this OU
            accounts_in_ou = []
            try:
                paginator = client.get_paginator('list_accounts_for_parent')
                for page in paginator.paginate(ParentId=ou_id):
                    for account in page['Accounts']:
                        if account['Status'] == 'ACTIVE':
                            accounts_in_ou.append({
                                'Id': account['Id'],
                                'Name': account['Name']
                            })
            except Exception as list_e:
                error_print(f"Error listing accounts for OU {ou_id}: {str(list_e)}")
            
            # Get child OUs
            child_ous = []
            try:
                paginator = client.get_paginator('list_organizational_units_for_parent')
                for page in paginator.paginate(ParentId=ou_id):
                    child_ous.extend([child['Id'] for child in page['OrganizationalUnits']])
            except Exception as ou_e:
                error_print(f"Error listing child OUs for OU {ou_id}: {str(ou_e)}")
            
            # Recursively get accounts from child OUs
            for child_ou in child_ous:
                accounts_in_ou.extend(get_accounts_in_ou(child_ou))
            
            return accounts_in_ou
        
        # Get accounts in the specified OU and its children
        accounts = get_accounts_in_ou(ou_id)
    
    except Exception as e:
        error_print(f"Failed to get accounts for OU {ou_id}: {str(e)}")
    
    return root_account_id, accounts

def get_accounts_from_file(filename):
    """Get AWS accounts from a file"""
    accounts = []
    
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            for line in file:
                account_id = line.strip()
                if account_id and not account_id.startswith('#'):
                    accounts.append({
                        'Id': account_id,
                        'Name': account_id
                    })
    except Exception as e:
        error_print(f"Failed to read accounts from file {filename}: {str(e)}")
    
    return accounts

def assume_role(account_id, current_account_id, root_account_id, role_name):
    """Assume IAM role in another account"""
    if not account_id or not role_name:
        return None
    
    if account_id == current_account_id or account_id == root_account_id:
        try:
            session = boto3.Session()
            credentials = session.get_credentials()
            credentials = credentials.get_frozen_credentials()
            return {
                'AccessKeyId':     credentials.access_key,
                'SecretAccessKey': credentials.secret_key,
                'SessionToken':    credentials.token
            }
        except Exception as ex:  # pylint: disable=broad-exception-caught
            error_print(ex, account_id)
            return None
        
    try:
        client = boto3.client('sts')
        partition = get_partition_prefix()
        role_arn = f'arn:{partition}:iam::{account_id}:role/{role_name}'
        
        response = client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=f'EonSizingTool-{account_id}'
        )
        
        return {
            'aws_access_key_id': response['Credentials']['AccessKeyId'],
            'aws_secret_access_key': response['Credentials']['SecretAccessKey'],
            'aws_session_token': response['Credentials']['SessionToken']
        }
    except Exception as e:
        error_print(f"Failed to assume role in account {account_id}: {str(e)}")
        return None

def get_partition_prefix():
    """Get the appropriate AWS partition prefix for ARNs based on cli arguments"""
    if args.use_gov:
        return "aws-us-gov"
    elif args.use_china:
        return "aws-cn"
    else:
        return "aws"

def get_regions(session):
    """Get list of available AWS regions for the selected partition using the provided session

    Args:
        session (boto3.Session): The Boto3 session object to use.

    Returns:
        list: List of AWS region names
    """
    try:
        # Create a client from the provided session
        # Use the right endpoint for the partition
        if args.use_gov:
            ec2 = session.client('ec2', region_name='us-gov-west-1')
        elif args.use_china:
            ec2 = session.client('ec2', region_name='cn-north-1')
        else:
            ec2 = session.client('ec2', region_name='us-east-1')

        response = ec2.describe_regions()
        region_list = [region['RegionName'] for region in response['Regions']]
        verbose_print(f"Found {len(region_list)} regions")
        
        return region_list
    except Exception as e:
        error_print(f"Failed to get AWS regions: {str(e)}")
        
        # Fallback to static region lists based on partition
        if args.use_gov:
            fallback_regions = ['us-gov-east-1', 'us-gov-west-1']
        elif args.use_china:
            fallback_regions = ['cn-north-1', 'cn-northwest-1']
        else:
            fallback_regions = ['us-east-1', 'us-east-2', 'us-west-1', 'us-west-2']
        
        verbose_print(f"Using fallback region list: {fallback_regions}")
        return fallback_regions

def get_account_alias(session, account_id):
    """Return the AWS account alias for the provided session/account."""
    try:
        iam = session.client('iam')
        response = iam.list_account_aliases()
        aliases = response.get('AccountAliases', [])
        if aliases:
            return aliases[0]
        return ''
    except Exception as e:
        # Do not fail the run; log verbose only as requested
        verbose_print(f"Could not retrieve account alias for account {account_id}: {str(e)}")
        return ''

def _list_instances_and_get_volumes(ec2, region, account_id):
    """Lists non-terminated instances and collects associated volume IDs."""
    verbose_print(f"Listing EC2 instances and volumes in {region} for account {account_id}")
            
    instances = []
    volume_ids_to_describe = set()
    instance_volume_map = {}
    paginator_instances = ec2.get_paginator('describe_instances')
    
    try:
        for page in paginator_instances.paginate(Filters=[{'Name': 'instance-state-name', 'Values': ['pending', 'running', 'shutting-down', 'stopping', 'stopped']}]):
            for reservation in page['Reservations']:
                for instance in reservation['Instances']:
                    instance_id = instance['InstanceId']
                    instances.append(instance)
                    instance_volume_map[instance_id] = []
                    for volume in instance.get('BlockDeviceMappings', []):
                        if 'Ebs' in volume and 'VolumeId' in volume['Ebs']:
                            vol_id = volume['Ebs']['VolumeId']
                            volume_ids_to_describe.add(vol_id)
                            instance_volume_map[instance_id].append(vol_id)
    except Exception as e:
        # Log error but proceed, maybe some instances were listed
        error_print(f"Error listing EC2 instances or volumes: {e}", account_id)
        
    return instances, list(volume_ids_to_describe), instance_volume_map

def _batch_describe_volumes(ec2, volume_id_list, account_id, region):
    """Describes volumes in batches."""
    volume_details = {}
    batch_size = 100
    for i in range(0, len(volume_id_list), batch_size):
        batch_ids = volume_id_list[i:i + batch_size]
        if not batch_ids:
            continue
        try:
            verbose_print(f"Describing volume batch {i // batch_size + 1} ({len(batch_ids)} volumes) in {region}")
            volume_response = ec2.describe_volumes(VolumeIds=batch_ids)
            for vol in volume_response['Volumes']:
                volume_details[vol['VolumeId']] = vol
        except ClientError as vol_e:
            if vol_e.response['Error']['Code'] == 'InvalidVolume.NotFound':
                error_print(f"Some volumes in batch not found: {batch_ids}. Error: {vol_e}", account_id)
            else:
                 error_print(f"Error describing volume batch {batch_ids}: {vol_e}", account_id)
        except Exception as vol_e:
            error_print(f"Error describing volume batch {batch_ids}: {vol_e}", account_id)
    return volume_details

def _batch_describe_snapshots_by_volume(ec2, volume_id_list, account_id, region):
    """Describes snapshots associated with the given volume IDs in batches."""
    snapshots_by_volume_id = {vol_id: [] for vol_id in volume_id_list}
    paginator_snapshots = ec2.get_paginator('describe_snapshots')
    snapshot_batch_size = 50
    
    for i in range(0, len(volume_id_list), snapshot_batch_size):
        batch_volume_ids = volume_id_list[i:i + snapshot_batch_size]
        if not batch_volume_ids:
            continue
        try:
            verbose_print(f"Describing snapshot batch for {len(batch_volume_ids)} volumes (Batch {i // snapshot_batch_size + 1}) in {region}")
            snapshot_pages = paginator_snapshots.paginate(
                OwnerIds=['self'],
                Filters=[{'Name': 'volume-id', 'Values': batch_volume_ids}]
            )
            for page in snapshot_pages:
                for snap in page['Snapshots']:
                    vol_id = snap.get('VolumeId')
                    if vol_id in snapshots_by_volume_id:
                        snapshots_by_volume_id[vol_id].append(snap)
        except ClientError as snap_e:
             error_print(f"Error describing snapshot batch for volumes {batch_volume_ids}: {snap_e}", account_id)
        except Exception as snap_e:
             error_print(f"Error describing snapshot batch for volumes {batch_volume_ids}: {snap_e}", account_id)
    return snapshots_by_volume_id

def _process_instance_inventory(instance, volume_details, snapshots_by_volume_id, instance_volume_map, account_id, region):
    """Processes a single instance's data and updates global inventory/summary."""
    instance_id = instance['InstanceId']
    # Calculate total storage using the pre-fetched details
    total_storage = 0
    instance_attached_volume_ids = instance_volume_map.get(instance_id, [])
    for vol_id in instance_attached_volume_ids:
        if vol_id in volume_details:
            size_gb = volume_details[vol_id].get('Size', 0)
            total_storage += size_gb
        else:
            verbose_print(f"Details not found for volume {vol_id} attached to instance {instance_id}")
    
    # Collect snapshots associated with this instance's volumes
    instance_snapshots = []
    for vol_id in instance_attached_volume_ids:
         instance_snapshots.extend(snapshots_by_volume_id.get(vol_id, []))
    
    # Process snapshots for dates
    snapshot_dates = [snap['StartTime'] for snap in instance_snapshots]
    first_date = min(snapshot_dates) if snapshot_dates else None
    latest_date = max(snapshot_dates) if snapshot_dates else None
    first_date_str = first_date.isoformat() if first_date else None
    latest_date_str = latest_date.isoformat() if latest_date else None
    
    # Process instance tags 
    instance_tags = {tag['Key']: tag['Value'] for tag in instance.get('Tags', [])}
    
    # Add instance to inventory
    add_to_inventory(
        account_id=account_id,
        resource_type='EC2',
        resource_id=instance_id,
        region=region,
        source_size=total_storage,
        snapshot_count=len(instance_snapshots),
        first_snapshot_date=first_date_str,
        latest_snapshot_date=latest_date_str,
        tags=instance_tags
    )
    
    # Add collected snapshots to global snapshot list
    for snap in instance_snapshots:
        snap_id = snap['SnapshotId']
        snap_size = snap.get('VolumeSize', 0)
        snapshot_tags = {tag['Key']: tag['Value'] for tag in snap.get('Tags', [])}
        vol_id = snap.get('VolumeId') # Get the Volume ID
        full_snapshot_size_bytes = snap.get('FullSnapshotSizeInBytes', None)  # Get full snapshot size in bytes
        
        add_to_snapshots(
            account_id=account_id,
            resource_type='EC2',
            snapshot_id=snap_id,
            region=region,
            snapshot_date=snap['StartTime'].isoformat(),
            snapshot_size=snap_size,
            source_resource_id=vol_id, # Pass Volume ID here
            tags=snapshot_tags,
            full_snapshot_size_bytes=full_snapshot_size_bytes
        )
    
    # Update summary counts (Note: Instance count is updated in the calling function)
    summary['EC2']['storage'] += total_storage
    summary['EC2']['snapshots'] += len(instance_snapshots)

def get_ec2_resources(account_id, region, session):
    """Get EC2 instances, associated volumes, and snapshots, using the provided session."""
    try:
        # Use the provided session
        ec2 = session.client('ec2', region_name=region)

        # Step 1: List Instances and Collect Volume IDs
        instances, volume_id_list, instance_volume_map = _list_instances_and_get_volumes(ec2, region, account_id)
        if not instances:
            verbose_print(f"No instances found in {region}.")
            return 0
        verbose_print(f"Found {len(instances)} instances and {len(volume_id_list)} unique volumes in {region}")

        # Step 2: Batch Describe Volumes
        volume_details = {}
        if volume_id_list:
            volume_details = _batch_describe_volumes(ec2, volume_id_list, account_id, region)
            verbose_print(f"Retrieved details for {len(volume_details)} volumes in {region}.")

        # Step 3: Batch Describe Snapshots
        snapshots_by_volume_id = {}
        if volume_id_list:
            snapshots_by_volume_id = _batch_describe_snapshots_by_volume(ec2, volume_id_list, account_id, region)
            total_snapshots_found = sum(len(snaps) for snaps in snapshots_by_volume_id.values())
            verbose_print(f"Retrieved {total_snapshots_found} snapshots for volumes in {region}.")

        # Step 4: Process Instances, Volumes, and Snapshots
        verbose_print(f"Processing instance details and associating snapshots in {region}")
        instance_count = 0
        for instance in instances:
            _process_instance_inventory(instance, volume_details, snapshots_by_volume_id, instance_volume_map, account_id, region)
            instance_count += 1
            
        # Update instance count summary here to avoid double counting if processing was inside loop
        summary['EC2']['count'] += instance_count
        
        return instance_count
        
    except Exception as e:
        error_print(f"Failed to get EC2 resources in region {region}: {str(e)}", account_id)
        return 0

def get_rds_resources(account_id, region, session):
    """Get RDS databases and their snapshots using the provided session"""
    try:
        # Use the provided session
        rds = session.client('rds', region_name=region)

        # Get RDS instances
        paginator = rds.get_paginator('describe_db_instances')
        instances = []
        
        for page in paginator.paginate():
            instances.extend(page['DBInstances'])
        
        processed_instance_count = 0
        for instance in instances:
            db_id = instance['DBInstanceIdentifier']
            storage_gb = instance['AllocatedStorage']
            instance_tags = {tag['Key']: tag['Value'] for tag in instance.get('TagList', [])}

            snapshots = []
            try:
                snapshot_paginator = rds.get_paginator('describe_db_snapshots')
                for snap_page in snapshot_paginator.paginate(DBInstanceIdentifier=db_id):
                    snapshots.extend(snap_page['DBSnapshots'])
            except Exception as snap_e:
                error_print(f"Error getting snapshots for RDS instance {db_id}: {str(snap_e)}", account_id)
            
            # Process snapshots
            snapshot_dates = [snap['SnapshotCreateTime'] for snap in snapshots if 'SnapshotCreateTime' in snap]
            first_date = min(snapshot_dates) if snapshot_dates else None
            latest_date = max(snapshot_dates) if snapshot_dates else None
            
            # Convert datetime objects to strings for serialization
            first_date_str = first_date.isoformat() if first_date else None
            latest_date_str = latest_date.isoformat() if latest_date else None
            
            # Add to inventory
            add_to_inventory(
                account_id=account_id,
                resource_type='RDS',
                resource_id=db_id,
                region=region,
                source_size=storage_gb,
                snapshot_count=len(snapshots),
                first_snapshot_date=first_date_str,
                latest_snapshot_date=latest_date_str,
                tags=instance_tags
            )
            
            # Add snapshots to snapshot list
            for snap in snapshots:
                snap_size = snap.get('AllocatedStorage', 0)
                snap_date = snap.get('SnapshotCreateTime')
                snapshot_tags = {tag['Key']: tag['Value'] for tag in snap.get('TagList', [])}
                
                add_to_snapshots(
                    account_id=account_id,
                    resource_type='RDS',
                    snapshot_id=snap['DBSnapshotIdentifier'],
                    region=region,
                    snapshot_date=snap_date.isoformat() if snap_date else None,
                    snapshot_size=snap_size,
                    source_resource_id=db_id,
                    tags=snapshot_tags,
                    full_snapshot_size_bytes=None
                )
            
            # Update summary
            summary['RDS']['count'] += 1
            summary['RDS']['storage'] += storage_gb
            summary['RDS']['snapshots'] += len(snapshots)
            processed_instance_count += 1
        
        return processed_instance_count
    except Exception as e:
        error_print(f"Failed to get RDS resources in region {region}: {str(e)}", account_id)
        return 0

def get_dynamodb_resources(account_id, region, session):
    """Get DynamoDB tables and their size using the provided session"""
    try:
        # Use the provided session
        dynamodb = session.client('dynamodb', region_name=region)
        cloudwatch = session.client('cloudwatch', region_name=region)
        
        # Get all tables
        paginator = dynamodb.get_paginator('list_tables')
        tables = []
        
        for page in paginator.paginate():
            tables.extend(page['TableNames'])
        
        # Set up time boundaries for CloudWatch metrics (fallback only)
        end_time = datetime.datetime.now(datetime.timezone.utc)
        start_time = end_time - datetime.timedelta(days=2)
        
        for table_name in tables:
            # Get table details
            table = dynamodb.describe_table(TableName=table_name)['Table']
            
            # Get table tags
            table_tags = {}
            try:
                partition = get_partition_prefix()
                resource_arn = f"arn:{partition}:dynamodb:{region}:{account_id}:table/{table_name}"
                tags_response = dynamodb.list_tags_of_resource(ResourceArn=resource_arn)
                if 'Tags' in tags_response:
                    table_tags = {tag['Key']: tag['Value'] for tag in tags_response['Tags']}
            except Exception as tag_e:
                error_print(f"Could not get tags for DynamoDB table {table_name}: {str(tag_e)}", account_id)
            
            # Primary method: Get table size from describe_table API
            size_gb = 0
            size_source = "describe_table"
            
            try:
                # Get size from describe_table response
                table_size_bytes = table.get('TableSizeBytes', 0)
                if table_size_bytes > 0:
                    size_gb = bytes_to_gb(table_size_bytes)
                    verbose_print(f"DynamoDB table {table_name} size from describe_table: {format_size_gb(size_gb)} GB")
                else:
                    verbose_print(f"DynamoDB table {table_name} has 0 size in describe_table, trying CloudWatch fallback...")
                    raise ValueError("TableSizeBytes is 0 or missing, trying CloudWatch fallback")
                    
            except Exception as describe_e:
                # Fallback method: Get table size from CloudWatch metrics
                verbose_print(f"Using CloudWatch fallback for table {table_name}: {str(describe_e)}")
                size_source = "cloudwatch"
                
                try:
                    size_response = cloudwatch.get_metric_statistics(
                        Namespace='AWS/DynamoDB',
                        MetricName='TableSizeBytes',
                        Dimensions=[{'Name': 'TableName', 'Value': table_name}],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=SECONDS_PER_DAY,  # Use constant
                        Statistics=['Maximum']
                    )
                    
                    # Get the most recent value
                    size_bytes = 0
                    if size_response['Datapoints']:
                        size_bytes = max(point['Maximum'] for point in size_response['Datapoints'])
                        verbose_print(f"DynamoDB table {table_name} size from CloudWatch: {format_size_gb(bytes_to_gb(size_bytes))} GB")
                    else:
                        verbose_print(f"No CloudWatch data found for table {table_name}, using 0 GB")
                    
                    # Convert bytes to GB
                    size_gb = bytes_to_gb(size_bytes)
                    
                except Exception as cw_e:
                    error_print(f"Both describe_table and CloudWatch failed for table {table_name}: {str(cw_e)}", account_id)
                    size_gb = 0
                    size_source = "failed"
            
            # Log which method was used for debugging
            verbose_print(f"Table {table_name} size: {format_size_gb(size_gb)} GB (source: {size_source})")
            
            # Get backups
            backup_response = dynamodb.list_backups(TableName=table_name)
            backups = backup_response.get('BackupSummaries', [])
            
            # Process backups
            backup_dates = [backup['BackupCreationDateTime'] for backup in backups]
            first_date = min(backup_dates) if backup_dates else None
            latest_date = max(backup_dates) if backup_dates else None
            
            # Convert datetime objects to strings for serialization
            first_date_str = first_date.isoformat() if first_date else None
            latest_date_str = latest_date.isoformat() if latest_date else None
            
            # Add to inventory
            add_to_inventory(
                account_id=account_id,
                resource_type='DynamoDB',
                resource_id=table_name,
                region=region,
                source_size=size_gb,
                snapshot_count=len(backups),
                first_snapshot_date=first_date_str,
                latest_snapshot_date=latest_date_str,
                tags=table_tags
            )
            
            # Add backups to snapshot list
            for backup in backups:
                # DynamoDB native backups do not support tagging, so we skip tag retrieval
                # Reference: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Backup-and-Restore.html
                backup_tags = {}
                verbose_print(f"DynamoDB backup {backup.get('BackupArn', 'unknown')} - native backups do not support tags")
                
                add_to_snapshots(
                    account_id=account_id,
                    resource_type='DynamoDB',
                    snapshot_id=backup['BackupArn'].split('/')[-1],
                    region=region,
                    snapshot_date=backup['BackupCreationDateTime'].isoformat(),
                    snapshot_size=size_gb,  # Assume backup is same size as table
                    source_resource_id=table_name, # Pass table name
                    tags=backup_tags,
                    full_snapshot_size_bytes=None
                )
            
            # Update summary
            summary['DynamoDB']['count'] += 1
            summary['DynamoDB']['storage'] += size_gb
            summary['DynamoDB']['snapshots'] += len(backups)
        
        return len(tables)
    except Exception as e:
        error_print(f"Failed to get DynamoDB resources in region {region}: {str(e)}", account_id)
        return 0

def get_eks_resources(account_id, region, session):
    """Get EKS clusters and associated EBS volumes using the provided session"""
    try:
        # Use the provided session
        eks = session.client('eks', region_name=region)
        ec2 = session.client('ec2', region_name=region)
        
        # Get all EKS clusters
        clusters = []
        try:
            paginator = eks.get_paginator('list_clusters')
            for page in paginator.paginate():
                clusters.extend(page['clusters'])
        except Exception as list_e:
            error_print(f"Error listing EKS clusters: {str(list_e)}", account_id)
            return 0
        
        total_volumes = 0
        total_storage = 0
        processed_cluster_count = 0
        
        for cluster_name in clusters:
            try:
                # Get cluster details
                cluster = eks.describe_cluster(name=cluster_name)['cluster']
                
                # Get cluster tags
                cluster_tags = {}
                if 'tags' in cluster:
                    cluster_tags = cluster['tags']
                
                # 1. Find instances and collect all associated volume IDs for this cluster
                instances_in_cluster = []
                cluster_volume_ids = set()
                instance_volume_map = {} # Store which volumes belong to which instance
                
                paginator_instances = ec2.get_paginator('describe_instances')
                instance_filters = [
                    {'Name': 'tag:eks:cluster-name', 'Values': [cluster_name]},
                    {'Name': 'instance-state-name', 'Values': ['pending', 'running', 'shutting-down', 'stopping', 'stopped']}
                ]

                verbose_print(f"Listing instances for EKS cluster {cluster_name} in {region}")
                try:
                    for page in paginator_instances.paginate(Filters=instance_filters):
                        for reservation in page['Reservations']:
                            for instance in reservation['Instances']:
                                instances_in_cluster.append(instance)
                                instance_id = instance['InstanceId']
                                instance_volume_map[instance_id] = []
                                for volume in instance.get('BlockDeviceMappings', []):
                                    if 'Ebs' in volume and 'VolumeId' in volume['Ebs']:
                                        vol_id = volume['Ebs']['VolumeId']
                                        cluster_volume_ids.add(vol_id)
                                        instance_volume_map[instance_id].append(vol_id)
                except Exception as e:
                     error_print(f"Error listing instances for EKS cluster {cluster_name}: {e}", account_id)
                     continue # Skip this cluster if instance listing fails

                verbose_print(f"Found {len(instances_in_cluster)} instances and {len(cluster_volume_ids)} unique volumes for cluster {cluster_name}")

                # 2. Batch describe the collected volumes
                volume_details = {}
                if cluster_volume_ids:
                    # Reuse the existing batch describe function
                    volume_details = _batch_describe_volumes(ec2, list(cluster_volume_ids), account_id, region) 
                    verbose_print(f"Retrieved details for {len(volume_details)} volumes for cluster {cluster_name}")
                
                # 3. Calculate storage using the fetched volume details
                cluster_volumes = 0
                cluster_storage = 0
                
                for instance in instances_in_cluster:
                    instance_id = instance['InstanceId']
                    # Iterate through volumes known to be attached to this instance
                    for vol_id in instance_volume_map.get(instance_id, []):
                         if vol_id in volume_details:
                             # Get size from the pre-fetched details
                             size_gb = volume_details[vol_id].get('Size', 0)
                             cluster_volumes += 1
                             cluster_storage += size_gb
                         else:
                             verbose_print(f"Details not found for volume {vol_id} attached to instance {instance_id} in cluster {cluster_name}")
                
                # Add to inventory
                add_to_inventory(
                    account_id=account_id,
                    resource_type='EKS',
                    resource_id=cluster_name,
                    region=region,
                    source_size=cluster_storage, # Use calculated storage
                    snapshot_count=0, # EKS volume snapshots are handled under EC2
                    tags=cluster_tags
                )
                
                # Update summary (use calculated values)
                total_volumes += cluster_volumes
                total_storage += cluster_storage
                processed_cluster_count += 1
                
            except Exception as cluster_e:
                error_print(f"Error processing EKS cluster {cluster_name}: {str(cluster_e)}", account_id)
        
        # Update overall summary (using count of successfully processed clusters)
        summary['EKS']['count'] += processed_cluster_count
        summary['EKS']['volumes'] += total_volumes
        summary['EKS']['storage'] += total_storage
        
        return processed_cluster_count # Return count of successfully processed clusters
    except Exception as e:
        error_print(f"Failed to get EKS resources in region {region}: {str(e)}", account_id)
        return 0

def get_efs_resources(account_id, region, session):
    """Get EFS file systems and their snapshots using the provided session"""
    try:
        # Use the provided session
        efs = session.client('efs', region_name=region)
        backup = session.client('backup', region_name=region)
        
        # Get all EFS file systems
        paginator = efs.get_paginator('describe_file_systems')
        file_systems = []
        
        for page in paginator.paginate():
            file_systems.extend(page['FileSystems'])
        
        for fs in file_systems:
            fs_id = fs['FileSystemId']
            # Convert bytes to GB
            size_gb = bytes_to_gb(fs['SizeInBytes']['Value'])
            
            # Get file system tags
            fs_tags = {}
            try:
                tags_response = efs.describe_tags(FileSystemId=fs_id)
                if 'Tags' in tags_response:
                    fs_tags = {tag['Key']: tag['Value'] for tag in tags_response['Tags']}
            except Exception as tag_e:
                error_print(f"Could not get tags for EFS file system {fs_id}: {str(tag_e)}", account_id)
            
            # Look for backups in AWS Backup
            snapshots = []
            try:
                # Query AWS Backup for recovery points
                backup_paginator = backup.get_paginator('list_recovery_points_by_resource')
                partition = get_partition_prefix()
                for backup_page in backup_paginator.paginate(
                    ResourceArn=f"arn:{partition}:elasticfilesystem:{region}:{account_id}:file-system/{fs_id}"
                ):
                    snapshots.extend(backup_page.get('RecoveryPoints', []))
            except Exception as backup_e:
                error_print(f"Error getting backups for EFS {fs_id}: {str(backup_e)}", account_id)
            
            # Process snapshots
            snapshot_dates = [snap['CreationDate'] for snap in snapshots if 'CreationDate' in snap]
            first_date = min(snapshot_dates) if snapshot_dates else None
            latest_date = max(snapshot_dates) if snapshot_dates else None
            
            # Convert datetime objects to strings for serialization
            first_date_str = first_date.isoformat() if first_date else None
            latest_date_str = latest_date.isoformat() if latest_date else None
            
            # Add to inventory
            add_to_inventory(
                account_id=account_id,
                resource_type='EFS',
                resource_id=fs_id,
                region=region,
                source_size=size_gb,
                snapshot_count=len(snapshots),
                first_snapshot_date=first_date_str,
                latest_snapshot_date=latest_date_str,
                tags=fs_tags
            )
            
            # Update summary
            summary['EFS']['count'] += 1
            summary['EFS']['storage'] += size_gb # Summary still tracks source size
            summary['EFS']['snapshots'] += len(snapshots)
        
        return len(file_systems)
    except Exception as e:
        error_print(f"Failed to get EFS resources in region {region}: {str(e)}", account_id)
        return 0

def get_redshift_resources(account_id, region, session):
    """Get Redshift clusters and their snapshots, using the provided session."""
    try:
        # Use the provided session
        redshift = session.client('redshift', region_name=region)
        cloudwatch = session.client('cloudwatch', region_name=region)
        
        # Get all Redshift clusters
        paginator = redshift.get_paginator('describe_clusters')
        clusters = []
        
        for page in paginator.paginate():
            clusters.extend(page['Clusters'])
        
        # Set up time boundaries for CloudWatch metrics (e.g., last 2 days)
        end_time = datetime.datetime.now(datetime.timezone.utc)
        start_time = end_time - datetime.timedelta(days=2)
        
        processed_cluster_count = 0
        # Process each cluster to gather individual data and update counts/used storage
        for cluster in clusters:
            cluster_id = cluster['ClusterIdentifier']
            storage_gb = 0 # Initialize used storage
            total_capacity_gb = 0 # Initialize total capacity in GB
            percentage_used = 0

            # --- Get Total Storage Capacity from describe_clusters response --- 
            total_capacity_mb = cluster.get('TotalStorageCapacityInMegaBytes', 0)
            if total_capacity_mb > 0:
                total_capacity_gb = total_capacity_mb / 1024.0 # Convert MB to GB
                verbose_print(f"Redshift {cluster_id} Total Capacity: {format_size_gb(total_capacity_gb)} GB (from describe_clusters)")
            else:
                verbose_print(f"Redshift {cluster_id} Total Capacity not found in describe_clusters response.")
                error_print(f"Could not determine TotalStorageCapacityInMegaBytes for Redshift {cluster_id}. Skipping storage calculation.", account_id)
                continue # Move to the next cluster

            # --- Get Percentage Disk Space Used from CloudWatch --- 
            try:
                percentage_response = cloudwatch.get_metric_statistics(
                    Namespace='AWS/Redshift',
                    MetricName='PercentageDiskSpaceUsed',
                    Dimensions=[{'Name': 'ClusterIdentifier', 'Value': cluster_id}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=SECONDS_PER_DAY,
                    Statistics=['Maximum'] 
                )
                
                if percentage_response['Datapoints']:
                    latest_percentage_point = max(percentage_response['Datapoints'], key=lambda x: x['Timestamp'])
                    percentage_used = latest_percentage_point.get('Maximum', 0)
                    verbose_print(f"Redshift {cluster_id} Percentage Used: {percentage_used:.2f}%")
                else:
                    verbose_print(f"No CloudWatch PercentageDiskSpaceUsed data found for Redshift {cluster_id}. Assuming 0% usage for calculation.")
                    percentage_used = 0

            except Exception as cw_e:
                error_print(f"Could not get CloudWatch PercentageDiskSpaceUsed metric for Redshift {cluster_id}: {cw_e}. Assuming 0% usage for calculation.", account_id)
                percentage_used = 0

            # Calculate Used Storage using total capacity (GB) and percentage
            if total_capacity_gb > 0:
                storage_gb = (percentage_used / 100.0) * total_capacity_gb
                verbose_print(f"Calculated Redshift {cluster_id} *used* storage: {format_size_gb(storage_gb)} GB")
            else:
                 error_print(f"Cannot calculate used storage for Redshift {cluster_id} as total capacity is 0. Reporting size as 0.", account_id)
                 storage_gb = 0
            
            # Get cluster tags
            cluster_tags = {}
            try:
                if 'Tags' in cluster and cluster['Tags']:
                    cluster_tags = {tag['Key']: tag['Value'] for tag in cluster['Tags']}
                else:
                    verbose_print(f"Tags not found directly in describe_clusters for {cluster_id}, attempting describe_tags.")
                    partition = get_partition_prefix()
                    resource_arn = f"arn:{partition}:redshift:{region}:{account_id}:cluster:{cluster_id}"
                    tags_response = redshift.describe_tags(ResourceName=resource_arn)
                    if 'TaggedResources' in tags_response and tags_response['TaggedResources']:
                         cluster_tags = {tag['Key']: tag['Value'] for tag in tags_response['TaggedResources'][0].get('Tags', [])}
            except Exception as tag_e:
                error_print(f"Could not get tags for Redshift cluster {cluster_id}: {str(tag_e)}", account_id)

            snapshots = []
            try:
                snapshot_paginator = redshift.get_paginator('describe_cluster_snapshots')
                for snap_page in snapshot_paginator.paginate(ClusterIdentifier=cluster_id):
                    snapshots.extend(snap_page['Snapshots'])
            except Exception as snap_e:
                error_print(f"Error getting snapshots for Redshift cluster {cluster_id}: {str(snap_e)}", account_id)
            
            snapshot_dates = [snap['SnapshotCreateTime'] for snap in snapshots if 'SnapshotCreateTime' in snap]
            first_date = min(snapshot_dates) if snapshot_dates else None
            latest_date = max(snapshot_dates) if snapshot_dates else None
            first_date_str = first_date.isoformat() if first_date else None
            latest_date_str = latest_date.isoformat() if latest_date else None
            
            # Add individual cluster to inventory using calculated *used* storage
            add_to_inventory(
                account_id=account_id,
                resource_type='Redshift',
                resource_id=cluster_id,
                region=region,
                source_size=storage_gb, # This is the *used* size
                snapshot_count=len(snapshots),
                first_snapshot_date=first_date_str,
                latest_snapshot_date=latest_date_str,
                tags=cluster_tags
            )
            
            # Add individual snapshots to snapshot list
            for snap in snapshots:
                snap_date = snap.get('SnapshotCreateTime')
                snapshot_tags = {tag['Key']: tag['Value'] for tag in snap.get('Tags', [])}
                snap_size_gb = storage_gb # Estimate snapshot size based on *used* cluster size
                
                add_to_snapshots(
                    account_id=account_id,
                    resource_type='Redshift',
                    snapshot_id=snap['SnapshotIdentifier'],
                    region=region,
                    snapshot_date=snap_date.isoformat() if snap_date else None,
                    snapshot_size=snap_size_gb, 
                    source_resource_id=cluster_id, # Pass cluster ID
                    tags=snapshot_tags,
                    full_snapshot_size_bytes=None
                )
            
            # Update summary counts and total *used* storage
            summary['Redshift']['count'] += 1
            summary['Redshift']['storage'] += storage_gb 
            processed_cluster_count += 1
            # End of loop for individual clusters

        # After processing all clusters in the region, get aggregate storage info
        try:
            verbose_print(f"Getting aggregate Redshift storage info for account {account_id} in region {region}")
            storage_info = redshift.describe_storage()
            
            backup_mb = storage_info.get('TotalBackupSizeInMegaBytes', 0)
            provisioned_mb = storage_info.get('TotalProvisionedStorageInMegaBytes', 0)
            
            backup_gb = backup_mb / 1024.0
            provisioned_gb = provisioned_mb / 1024.0
            
            verbose_print(f"Account {account_id} Region {region} - Redshift Total Backup: {format_size_gb(backup_gb)} GB, Total Provisioned: {format_size_gb(provisioned_gb)} GB")

            # Add regional totals to the global summary

            summary['Redshift']['total_backup_size_gb'] += backup_gb
            summary['Redshift']['total_provisioned_storage_gb'] += provisioned_gb
            
        except Exception as storage_e:
            error_print(f"Could not get Redshift describe_storage info in region {region}: {str(storage_e)}", account_id)

        return processed_cluster_count # Return count of clusters successfully processed for inventory/snapshots
        
    except Exception as e:
        error_print(f"Failed to get Redshift resources in region {region}: {str(e)}", account_id)
        return 0

def get_docdb_resources(account_id, region, session):
    """Get DocumentDB clusters and their snapshots using the provided session"""
    try:
        # Use the provided session
        docdb = session.client('docdb', region_name=region)
        
        # Get all DocumentDB clusters
        paginator = docdb.get_paginator('describe_db_clusters')
        clusters = []
        
        for page in paginator.paginate():
            # Filter for only DocumentDB clusters (not Aurora)
            for cluster in page['DBClusters']:
                if cluster['Engine'] == 'docdb':
                    clusters.append(cluster)
        
        for cluster in clusters:
            cluster_id = cluster['DBClusterIdentifier']
            
            # Get cluster tags
            cluster_tags = {}
            try:
                tags_response = docdb.list_tags_for_resource(ResourceName=cluster['DBClusterArn'])
                if 'TagList' in tags_response:
                    cluster_tags = {tag['Key']: tag['Value'] for tag in tags_response['TagList']}
            except Exception as tag_e:
                error_print(f"Could not get tags for DocumentDB cluster {cluster_id}: {str(tag_e)}", account_id)
            
            # Get storage information
            storage_gb = cluster.get('AllocatedStorage', 0)
            
            # Get snapshots
            snapshots = []
            try:
                snapshot_paginator = docdb.get_paginator('describe_db_cluster_snapshots')
                for snap_page in snapshot_paginator.paginate(DBClusterIdentifier=cluster_id):
                    snapshots.extend(snap_page['DBClusterSnapshots'])
            except Exception as snap_e:
                error_print(f"Error getting snapshots for DocumentDB cluster {cluster_id}: {str(snap_e)}", account_id)
            
            # Process snapshots
            snapshot_dates = [snap['SnapshotCreateTime'] for snap in snapshots if 'SnapshotCreateTime' in snap]
            first_date = min(snapshot_dates) if snapshot_dates else None
            latest_date = max(snapshot_dates) if snapshot_dates else None
            
            # Convert datetime objects to strings for serialization
            first_date_str = first_date.isoformat() if first_date else None
            latest_date_str = latest_date.isoformat() if latest_date else None
            
            # Add to inventory
            add_to_inventory(
                account_id=account_id,
                resource_type='DocumentDB',
                resource_id=cluster_id,
                region=region,
                source_size=storage_gb,
                snapshot_count=len(snapshots),
                first_snapshot_date=first_date_str,
                latest_snapshot_date=latest_date_str,
                tags=cluster_tags
            )
            
            # Add snapshots to snapshot list
            for snap in snapshots:
                snap_date = snap.get('SnapshotCreateTime')
                
                # Get snapshot tags
                snapshot_tags = {}
                try:
                    if 'DBClusterSnapshotArn' in snap:
                        tags_response = docdb.list_tags_for_resource(ResourceName=snap['DBClusterSnapshotArn'])
                        if 'TagList' in tags_response:
                            snapshot_tags = {tag['Key']: tag['Value'] for tag in tags_response['TagList']}
                except Exception as tag_e:
                    error_print(f"Could not get tags for DocumentDB snapshot {snap['DBClusterSnapshotIdentifier']}: {str(tag_e)}", account_id)
                
                add_to_snapshots(
                    account_id=account_id,
                    resource_type='DocumentDB',
                    snapshot_id=snap['DBClusterSnapshotIdentifier'],
                    region=region,
                    snapshot_date=snap_date.isoformat() if snap_date else None,
                    snapshot_size=storage_gb,  # Assume backup is same size as cluster
                    source_resource_id=cluster_id, # Pass cluster ID
                    tags=snapshot_tags,
                    full_snapshot_size_bytes=None
                )
            
            # Update summary
            summary['DocumentDB']['count'] += 1
            summary['DocumentDB']['storage'] += storage_gb
            summary['DocumentDB']['snapshots'] += len(snapshots)
        
        return len(clusters)
    except Exception as e:
        error_print(f"Failed to get DocumentDB resources in region {region}: {str(e)}", account_id)
        return 0

def get_opensearch_resources(account_id, region, session):
    """Get OpenSearch domains and their snapshots using the provided session"""
    try:
        # Use the provided session

        # Use elasticsearch client as it works for both ElasticSearch and OpenSearch
        # Try OpenSearch client first
        try:
            opensearch = session.client('opensearch', region_name=region)
            domain_response = opensearch.list_domain_names()
        except ClientError as ce:
            # If opensearch client fails (e.g., endpoint not available), try elasticsearch
            if ce.response['Error']['Code'] == 'UnsupportedOperation': # Or other relevant error code
                error_print("OpenSearch client failed, trying legacy Elasticsearch client...", account_id)
                opensearch = session.client('elasticsearch', region_name=region)
                domain_response = opensearch.list_domain_names() # Try again with legacy client
            else:
                raise # Re-raise other client errors
        except Exception as list_e:
             error_print(f"Failed to list OpenSearch/ElasticSearch domains: {str(list_e)}", account_id)
             return 0

        domain_names = [domain['DomainName'] for domain in domain_response['DomainNames']]

        for domain_name in domain_names:
            try:
                # Get domain details
                domain = opensearch.describe_domain(DomainName=domain_name)['DomainStatus']
                
                # Get domain tags
                domain_tags = {}
                try:
                    partition = get_partition_prefix()
                    domain_arn = domain.get('ARN') or f"arn:{partition}:es:{region}:{account_id}:domain/{domain_name}"
                    tags_response = opensearch.list_tags(ARN=domain_arn)
                    if 'TagList' in tags_response:
                        domain_tags = {tag['Key']: tag['Value'] for tag in tags_response['TagList']}
                except Exception as tag_e:
                    error_print(f"Could not get tags for OpenSearch domain {domain_name}: {str(tag_e)}", account_id)
                
                # Calculate storage size based on instance type and count
                instance_type = domain.get('ClusterConfig', {}).get('InstanceType', '')
                instance_count = domain.get('ClusterConfig', {}).get('InstanceCount', 1)
                
                # Get EBS volume size if available
                storage_gb = 0   
                ebs_options = domain.get('EBSOptions', {})
                if ebs_options.get('EBSEnabled', False):
                    volume_size = ebs_options.get('VolumeSize', 0)
                    if volume_size > 0:
                        storage_gb = volume_size * instance_count
                
                # OpenSearch snapshots are typically stored in S3, so we don't have direct access
                # We'll assume no snapshots here
                
                # Add to inventory
                add_to_inventory(
                    account_id=account_id,
                    resource_type='OpenSearch',
                    resource_id=domain_name,
                    region=region,
                    source_size=storage_gb,
                    snapshot_count=0,
                    tags=domain_tags
                )
                
                # Update summary
                summary['OpenSearch']['count'] += 1
                summary['OpenSearch']['storage'] += storage_gb
                
            except Exception as domain_e:
                error_print(f"Error processing OpenSearch domain {domain_name}: {str(domain_e)}", account_id)
        
        return len(domain_names)
    except Exception as e:
        error_print(f"Failed to get OpenSearch resources in region {region}: {str(e)}", account_id)
        return 0

def get_fsx_resources(account_id, region, session):
    """Get FSx file systems and their backups using the provided session"""
    try:
        # Use the provided session
        fsx = session.client('fsx', region_name=region)
        
        # Get all FSx file systems
        paginator = fsx.get_paginator('describe_file_systems')
        file_systems = []
        
        for page in paginator.paginate():
            file_systems.extend(page['FileSystems'])
        
        for fs in file_systems:
            fs_id = fs['FileSystemId']
            storage_gb = fs['StorageCapacity']
            
            # Get file system tags
            fs_tags = {}
            if 'Tags' in fs:
                fs_tags = {tag['Key']: tag['Value'] for tag in fs['Tags']}
            
            # Get backups
            backups = []
            try:
                backup_paginator = fsx.get_paginator('describe_backups')
                for backup_page in backup_paginator.paginate(Filters=[{'Name': 'file-system-id', 'Values': [fs_id]}]):
                    backups.extend(backup_page['Backups'])
            except Exception as backup_e:
                error_print(f"Error getting backups for FSx {fs_id}: {str(backup_e)}", account_id)
            
            # Process backups
            backup_dates = [backup['CreationTime'] for backup in backups if 'CreationTime' in backup]
            first_date = min(backup_dates) if backup_dates else None
            latest_date = max(backup_dates) if backup_dates else None
            
            # Convert datetime objects to strings for serialization
            first_date_str = first_date.isoformat() if first_date else None
            latest_date_str = latest_date.isoformat() if latest_date else None
            
            # Add to inventory
            add_to_inventory(
                account_id=account_id,
                resource_type='FSx',
                resource_id=fs_id,
                region=region,
                source_size=storage_gb,
                snapshot_count=len(backups),
                first_snapshot_date=first_date_str,
                latest_snapshot_date=latest_date_str,
                tags=fs_tags
            )
            
            # Add backups to snapshot list
            for backup in backups:
                backup_date = backup.get('CreationTime')
                
                # Get backup tags
                backup_tags = {}
                if 'Tags' in backup:
                    backup_tags = {tag['Key']: tag['Value'] for tag in backup['Tags']}
                
                add_to_snapshots(
                    account_id=account_id,
                    resource_type='FSx',
                    snapshot_id=backup['BackupId'],
                    region=region,
                    snapshot_date=backup_date.isoformat() if backup_date else None,
                    snapshot_size=storage_gb,  # Assume backup is same size as file system
                    source_resource_id=fs_id, # Pass file system ID
                    tags=backup_tags,
                    full_snapshot_size_bytes=None
                )
            
            # Update summary
            summary['FSx']['count'] += 1
            summary['FSx']['storage'] += storage_gb
            summary['FSx']['snapshots'] += len(backups)
        
        return len(file_systems)
    except Exception as e:
        error_print(f"Failed to get FSx resources in region {region}: {str(e)}", account_id)
        return 0

def get_s3_resources(account_id, session):
    """Get S3 buckets and their objects using the provided session"""
    try:
        # Use the provided session
        s3 = session.client('s3') # S3 client is typically not region-specific for list_buckets

        # Get all buckets
        response = s3.list_buckets()
        buckets = response['Buckets']
        if not buckets:
            return 0

        current_time = datetime.datetime.now(datetime.timezone.utc)

        # Thread-local client caches to avoid recreating clients for each bucket
        thread_local_clients = threading.local()

        def _get_global_s3_client():
            if not hasattr(thread_local_clients, 's3_global'):
                thread_local_clients.s3_global = session.client('s3')
            return thread_local_clients.s3_global

        def _get_regional_clients(region):
            if not hasattr(thread_local_clients, 'regional'):
                thread_local_clients.regional = {}
            if region not in thread_local_clients.regional:
                thread_local_clients.regional[region] = {
                    's3': session.client('s3', region_name=region),
                    'cloudwatch': session.client('cloudwatch', region_name=region)
                }
            regional = thread_local_clients.regional[region]
            return regional['s3'], regional['cloudwatch']

        # Worker to process a single bucket. Returns a tuple with results for main-thread consolidation.
        def _process_single_bucket(bucket_name: str):
            try:
                verbose_print(f"Processing bucket: {bucket_name}")

                # Use cached, thread-local S3 global client for location (global service)
                global_s3 = _get_global_s3_client()
                location = global_s3.get_bucket_location(Bucket=bucket_name)
                region = location['LocationConstraint'] or 'us-east-1'

                # Get cached regional clients in this thread
                s3_regional, cloudwatch = _get_regional_clients(region)

                # Set up time boundaries for CloudWatch metrics
                end_time = current_time
                start_time = end_time - datetime.timedelta(days=2)

                # Get bucket tags
                bucket_tags = {}
                try:
                    tags_response = s3_regional.get_bucket_tagging(Bucket=bucket_name)
                    if 'TagSet' in tags_response:
                        bucket_tags = {tag['Key']: tag['Value'] for tag in tags_response['TagSet']}
                except Exception as tag_e:
                    # NoSuchTagSet error is common for buckets without tags
                    if not str(tag_e).endswith('NoSuchTagSet'):
                        error_print(f"Could not get tags for S3 bucket {bucket_name}: {str(tag_e)}", account_id)

                # Get object count from CloudWatch metrics
                try:
                    count_response = cloudwatch.get_metric_statistics(
                        Namespace='AWS/S3',
                        MetricName='NumberOfObjects',
                        Dimensions=[
                            {'Name': 'BucketName', 'Value': bucket_name},
                            {'Name': 'StorageType', 'Value': 'AllStorageTypes'}
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=SECONDS_PER_DAY,
                        Statistics=['Average']
                    )

                    object_count = 0
                    if count_response['Datapoints']:
                        object_count = int(max(point['Average'] for point in count_response['Datapoints']))
                except Exception as cw_e:
                    error_msg = f"Could not get object count from CloudWatch for bucket {bucket_name}: {str(cw_e)}"
                    error_print(error_msg, account_id)
                    object_count = 0

                # Get total size across all storage classes from CloudWatch metrics
                total_size_bytes = 0
                storage_classes = [
                    'StandardStorage',
                    'StandardIAStorage',
                    'StandardInfrequentAccessStorage',
                    'OneZoneIAStorage',
                    'ReducedRedundancyStorage',
                    'GlacierStorage',
                    'GlacierDeepArchiveStorage',
                    'IntelligentTieringStorage',
                    'IntelligentTieringAAStorage',
                    'IntelligentTieringAIAStorage',
                    'IntelligentTieringDAAStorage'
                ]

                for storage_class in storage_classes:
                    try:
                        size_response = cloudwatch.get_metric_statistics(
                            Namespace='AWS/S3',
                            MetricName='BucketSizeBytes',
                            Dimensions=[
                                {'Name': 'BucketName', 'Value': bucket_name},
                                {'Name': 'StorageType', 'Value': storage_class}
                            ],
                            StartTime=start_time,
                            EndTime=end_time,
                            Period=SECONDS_PER_DAY,
                            Statistics=['Average']
                        )

                        if size_response['Datapoints']:
                            latest_size = max(point['Average'] for point in size_response['Datapoints'])
                            total_size_bytes += latest_size
                            verbose_print(f"Bucket {bucket_name} {storage_class}: {bytes_to_gb(latest_size):.2f} GB")
                    except Exception as sc_e:
                        error_msg = f"Could not get {storage_class} size for bucket {bucket_name}: {str(sc_e)}"
                        error_print(error_msg, account_id)

                total_size_gb = bytes_to_gb(total_size_bytes)

                # Get S3 Inventory configurations for this bucket
                inventory_configs = []
                try:
                    inventory_response = s3_regional.list_bucket_inventory_configurations(Bucket=bucket_name)
                    if 'InventoryConfigurationList' in inventory_response:
                        for config in inventory_response['InventoryConfigurationList']:
                            s3_dest = config.get('Destination', {}).get('S3BucketDestination', {})
                            inventory_config = {
                                'id': config.get('Id', ''),
                                'enabled': config.get('IsEnabled', False),
                                'frequency': config.get('Schedule', {}).get('Frequency', ''),
                                'destination_bucket': s3_dest.get('Bucket', ''),
                                'destination_account': s3_dest.get('AccountId', ''),
                                'format': s3_dest.get('Format', ''),
                                'prefix': s3_dest.get('Prefix', ''),
                                'included_object_versions': config.get('IncludedObjectVersions', ''),
                                'optional_fields': config.get('OptionalFields', [])
                            }
                            inventory_configs.append(inventory_config)
                        verbose_print(f"Found {len(inventory_configs)} S3 Inventory configurations for bucket {bucket_name}")
                except Exception as inv_e:
                    # S3 Inventory configurations might not be accessible or configured
                    verbose_print(f"Could not get S3 Inventory configurations for bucket {bucket_name}: {str(inv_e)}")

                return {
                    'bucket_name': bucket_name,
                    'region': region,
                    'object_count': object_count,
                    'total_size_gb': total_size_gb,
                    'tags': bucket_tags,
                    's3_inventory_configs': inventory_configs
                }

            except Exception as bucket_e:
                error_print(f"Could not process bucket {bucket_name}: {str(bucket_e)}", account_id)
                return None

        max_workers = min(5, len(buckets))
        results_count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_bucket = {
                executor.submit(_process_single_bucket, bucket['Name']): bucket['Name']
                for bucket in buckets
            }

            for future in concurrent.futures.as_completed(future_to_bucket):
                result = future.result()
                if not result:
                    continue

                add_to_inventory(
                    account_id=account_id,
                    resource_type='S3',
                    resource_id=result['bucket_name'],
                    region=result['region'],
                    source_size=result['total_size_gb'],
                    snapshot_count=0,
                    tags=result['tags'],
                    object_count=result['object_count'],
                    s3_inventory_configs=result.get('s3_inventory_configs', [])
                )

                summary['S3']['count'] += 1
                summary['S3']['objects'] += result['object_count']
                summary['S3']['storage'] += result['total_size_gb']
                
                # Track S3 Inventory configurations
                if result.get('s3_inventory_configs'):
                    summary['S3']['inventory_enabled_count'] += 1
                    summary['S3']['total_inventory_configs'] += len(result['s3_inventory_configs'])
                
                results_count += 1

        return results_count
    except Exception as e:
        error_print(f"Failed to get S3 resources: {str(e)}", account_id)
        return 0

def _collect_recovery_points_and_arns(backup_client, region, account_id):
    """Lists vaults and recovery points, collecting initial data and ARNs."""
    regional_recovery_points_data = [] 
    regional_recovery_point_arns = []
    vaults_found = False
    
    try:
        vaults_response = backup_client.list_backup_vaults()
        if vaults_response['BackupVaultList']:
            vaults_found = True
            for vault in vaults_response['BackupVaultList']:
                vault_name = vault['BackupVaultName']
                vault_arn = vault['BackupVaultArn']
                try:
                    paginator = backup_client.get_paginator('list_recovery_points_by_backup_vault')
                    for page in paginator.paginate(BackupVaultName=vault_name):
                        for recovery_point in page.get('RecoveryPoints', []):
                            rp_arn = recovery_point.get('RecoveryPointArn', '')
                            if not rp_arn: continue
                            
                            recovery_point_data = {
                                'account_id': account_id,
                                'region': region,
                                'vault_arn': vault_arn,
                                'recovery_point_arn': rp_arn,
                                'resource_arn': recovery_point.get('ResourceArn', ''),
                                'resource_type': recovery_point.get('ResourceType', ''),
                                'creation_date': recovery_point.get('CreationDate').isoformat() if recovery_point.get('CreationDate') else '',
                                'status': recovery_point.get('Status', ''),
                                'backup_size_bytes': recovery_point.get('BackupSizeInBytes', 0),
                                'backup_size_gb': bytes_to_gb(recovery_point['BackupSizeInBytes']) if recovery_point.get('BackupSizeInBytes') else 0,
                                'lifecycle': recovery_point.get('Lifecycle', {}),
                                'tags': {}, # Initialize tags
                                'calculated_delete_at': recovery_point.get('CalculatedLifecycle', {}).get('DeleteAt', None),
                                'calculated_move_to_cold_at': recovery_point.get('CalculatedLifecycle', {}).get('MoveToColdStorageAt', None),
                                'last_restore_time': recovery_point.get('LastRestoreTime', None),
                                'parent_recovery_point_arn': recovery_point.get('ParentRecoveryPointArn', ''),
                                'is_parent': recovery_point.get('IsParent', False),
                                'vault_type': recovery_point.get('VaultType', '')
                            }
                            # Convert datetimes to ISO strings if they exist
                            if recovery_point_data['calculated_delete_at']:
                                recovery_point_data['calculated_delete_at'] = recovery_point_data['calculated_delete_at'].isoformat()
                            if recovery_point_data['calculated_move_to_cold_at']:
                                recovery_point_data['calculated_move_to_cold_at'] = recovery_point_data['calculated_move_to_cold_at'].isoformat()
                            if recovery_point_data['last_restore_time']:
                                recovery_point_data['last_restore_time'] = recovery_point_data['last_restore_time'].isoformat()

                            regional_recovery_point_arns.append(rp_arn)
                            regional_recovery_points_data.append(recovery_point_data)
                except Exception as vault_e:
                    # Log error for a specific vault but continue with others
                    error_print(f"Error getting recovery points for vault {vault_name} in {region}: {str(vault_e)}", account_id)
    except Exception as list_vaults_e:
         error_print(f"Could not list backup vaults in region {region}: {str(list_vaults_e)}", account_id)
         
    return vaults_found, regional_recovery_point_arns, regional_recovery_points_data

def _collect_indexed_recovery_points(backup_client, region, account_id):
    """Collects index status for all indexed recovery points in the region."""
    index_status_map = {}
    verbose_print(f"Collecting indexed recovery point statuses for region {region}...")
    try:
        paginator = backup_client.get_paginator('list_indexed_recovery_points')
        for page in paginator.paginate():
            for rp in page.get('IndexedRecoveryPoints', []):
                arn = rp.get('RecoveryPointArn')
                if arn:
                    index_status_map[arn] = {
                        'index_status': rp.get('IndexStatus', ''),
                        'index_status_message': rp.get('IndexStatusMessage', '')
                    }
        verbose_print(f"Collected index status for {len(index_status_map)} recovery points in {region}.")
    except Exception as index_e:
        error_print(f"Could not list indexed recovery points in region {region}: {str(index_e)}", account_id)
    return index_status_map

def _fetch_tags_in_batches(tagging_client, region, account_id, arns):
    """Fetches tags for a list of ARNs in batches using ResourceGroupsTaggingAPI."""
    verbose_print(f"Fetching tags for {len(arns)} resources in {region} using batch API...")
    tags_by_arn = {}
    batch_size = 100 # Max ARNs per get_resources call
    
    try:
        for i in range(0, len(arns), batch_size):
            batch_arns = arns[i:i + batch_size]
            verbose_print(f"  Fetching tags for batch {i // batch_size + 1} ({len(batch_arns)} ARNs)")
            try:
                # Use paginator for get_resources as it might return multiple pages for a single batch call
                tag_paginator = tagging_client.get_paginator('get_resources')
                tag_pages = tag_paginator.paginate(ResourceARNList=batch_arns)
                
                for page in tag_pages:
                    for resource_mapping in page.get('ResourceTagMappingList', []):
                        arn = resource_mapping.get('ResourceARN')
                        tags = {tag['Key']: tag['Value'] for tag in resource_mapping.get('Tags', [])}
                        if arn:
                            tags_by_arn[arn] = tags
                            
            except ClientError as tag_batch_error:
                 error_print(f"API Error fetching tags batch {i // batch_size + 1} in {region}: {tag_batch_error}", account_id)
            except Exception as tag_batch_error:
                 error_print(f"Unexpected error fetching tags batch {i // batch_size + 1} in {region}: {tag_batch_error}", account_id)
                 
    except Exception as tagging_api_e:
         error_print(f"Failed to use ResourceGroupsTaggingAPI in region {region}: {str(tagging_api_e)}", account_id)
         # Return potentially partially filled dict or empty dict on error

    verbose_print(f"Finished fetching tags for {len(tags_by_arn)} resources in {region}.")
    return tags_by_arn

def _collect_backup_plans_and_selections(backup_client, region, account_id):
    """Lists backup plans and selections, appending details to global backup_data."""
    plans_found = False
    try:
        plans_response = backup_client.list_backup_plans()
        if plans_response['BackupPlansList']:
            plans_found = True
            for plan_summary in plans_response['BackupPlansList']:
                plan_id = plan_summary['BackupPlanId']
                plan_name = plan_summary['BackupPlanName']
                schedule_expr = '' # Initialize schedule expression

                # Get plan details to extract schedule
                try:
                    plan_details_response = backup_client.get_backup_plan(BackupPlanId=plan_id)
                    if 'BackupPlan' in plan_details_response and 'Rules' in plan_details_response['BackupPlan']:
                        rules = plan_details_response['BackupPlan']['Rules']
                        if rules: # Get schedule from the first rule
                            schedule_expr = rules[0].get('ScheduleExpression', '')
                except Exception as plan_details_e:
                    error_print(f"Error getting details for backup plan {plan_id}: {str(plan_details_e)}", account_id)

                try:
                    selections_response = backup_client.list_backup_selections(BackupPlanId=plan_id)
                    for selection in selections_response.get('BackupSelectionsList', []):
                        selection_id = selection['SelectionId']
                        selection_name = selection['SelectionName']
                        try:
                            selection_details = backup_client.get_backup_selection(
                                BackupPlanId=plan_id,
                                SelectionId=selection_id
                            )
                            if 'BackupSelection' in selection_details:
                                backup_selection = selection_details['BackupSelection']
                                backup_data.append({
                                    'account_id': account_id,
                                    'region': region,
                                    'plan_id': plan_id,
                                    'plan_name': plan_name,
                                    'selection_id': selection_id,
                                    'selection_name': selection_name,
                                    'schedule_expression': schedule_expr,
                                    'backup_selection': backup_selection
                                })

                        except Exception as selection_e:
                            error_print(f"Error getting backup selection details for {selection_id} in plan {plan_id}: {str(selection_e)}", account_id)
                except Exception as list_selections_e:
                    error_print(f"Error listing backup selections for plan {plan_id}: {str(list_selections_e)}", account_id)

    except Exception as list_plans_e:
        error_print(f"Could not list backup plans in region {region}: {str(list_plans_e)}", account_id)

    return plans_found

def get_backup_resources(account_id, region, session):
    """Check if AWS Backup is configured and collect backup plan details using the provided session."""
    backup_configured = False
    tagging_client = None
    
    try:
        # Use the provided session to create clients
        backup_client = session.client('backup', region_name=region)
        tagging_client = session.client('resourcegroupstaggingapi', region_name=region)

        # 1. Collect base recovery points and ARNs (iterating through vaults)
        vaults_found, all_arns, rp_data_list = _collect_recovery_points_and_arns(backup_client, region, account_id)
        backup_configured = vaults_found # Initially set based on vault presence

        # 2. Collect backup plans and selections
        plans_found = _collect_backup_plans_and_selections(backup_client, region, account_id)
        backup_configured = backup_configured or plans_found

        # 3. Collect indexed recovery point statuses (region-wide)
        index_status_map = _collect_indexed_recovery_points(backup_client, region, account_id)

        # 4. Fetch tags in batches if ARNs were found
        tags_by_arn = {}
        if all_arns:
            try:
                tags_by_arn = _fetch_tags_in_batches(tagging_client, region, account_id, all_arns)

                # 5. Merge tags and Index Status into recovery point data
                verbose_print(f"Merging tags and index status for {len(rp_data_list)} recovery points in {region}...")
                for rp_data in rp_data_list:
                    arn = rp_data['recovery_point_arn']

                    # Merge Tags
                    if arn in tags_by_arn:
                        rp_data['tags'] = tags_by_arn[arn]

                    # Merge Index Status
                    if arn in index_status_map:
                        rp_data['index_status'] = index_status_map[arn].get('index_status', '')
                        rp_data['index_status_message'] = index_status_map[arn].get('index_status_message', '')
                    else:
                        # Ensure keys exist even if not found in index map
                        rp_data['index_status'] = ''
                        rp_data['index_status_message'] = ''

                # 6. Append final data to global list
                backup_recovery_points.extend(rp_data_list)
                verbose_print(f"Finished merging tags and index status for recovery points in {region}.")

            except Exception as merge_e:
                 error_print(f"Error during tag/index status merging process in {region}: {merge_e}", account_id)
                 backup_accounts_with_errors.add(account_id)
                 # Ensure index status keys exist before appending
                 for rp_data in rp_data_list:
                     rp_data.setdefault('index_status', '')
                     rp_data.setdefault('index_status_message', '')
                 backup_recovery_points.extend(rp_data_list)
        else: # No ARNs found, just append the (empty) rp_data_list
             backup_recovery_points.extend(rp_data_list)

        # 7. Update overall configured status and summary
        if backup_configured:
            summary['AWS_Backup']['configured'] = True
            backup_accounts_with_config.add(account_id)

        return backup_configured
        
    except Exception as e:
        error_print(f"Failed to get AWS Backup resources in region {region}: {str(e)}", account_id)
        backup_accounts_with_errors.add(account_id)
        return False # Return False indicating configuration check failed or backup not configured

def aggregate_backup_storage_by_service():
    """Aggregate AWS Backup service storage by service type and update summary
    
    Note: This function specifically aggregates AWS Backup service storage.
    It does NOT include native backup storage (e.g., Redshift native backups,
    RDS automated backups, etc.) which are tracked separately.
    """
    verbose_print("Aggregating AWS Backup service storage by service type...")
    
    # Map AWS Backup resource types to our service categories
    resource_type_mapping = {
        'EC2': 'EC2',
        'EBS': 'EC2',  # EBS volumes are part of EC2
        'RDS': 'RDS',
        'DynamoDB': 'DynamoDB',
        'EFS': 'EFS',
        'FSx': 'FSx',
        'Storage Gateway': 'S3',  # Storage Gateway backups typically relate to S3
        'S3': 'S3',
        'DocumentDB': 'DocumentDB',
        'Neptune': 'DocumentDB',  # Group Neptune with DocumentDB for now
        'Redshift': 'Redshift',
        'Timestream': 'DynamoDB',  # Group Timestream with DynamoDB for now
        'CloudFormation': 'EC2',  # CloudFormation backups typically relate to EC2
        'SAP HANA on Amazon EC2': 'EC2',
        'Virtual Machine': 'EC2'  # VMware backups relate to EC2
    }
    
    # Aggregate backup storage by service
    backup_totals = {}
    for recovery_point in backup_recovery_points:
        resource_type = recovery_point.get('resource_type', '')
        backup_size_gb = recovery_point.get('backup_size_gb', 0)
        
        # Map to our service category
        service_category = resource_type_mapping.get(resource_type, 'Other')
        
        if service_category != 'Other' and backup_size_gb > 0:
            if service_category not in backup_totals:
                backup_totals[service_category] = 0
            backup_totals[service_category] += backup_size_gb
            
            verbose_print(f"Adding {format_size_gb(backup_size_gb)} GB backup storage to {service_category} (resource type: {resource_type})")
    
    # Update summary with backup storage totals
    for service, backup_storage in backup_totals.items():
        if service in summary:
            summary[service]['backup_storage'] += backup_storage
            verbose_print(f"Total backup storage for {service}: {format_size_gb(summary[service]['backup_storage'])} GB")
    
    if backup_totals:
        total_backup_storage = sum(backup_totals.values())
        verbose_print(f"Total AWS Backup storage across all services: {format_size_gb(total_backup_storage)} GB")
    else:
        verbose_print("No AWS Backup storage found to aggregate")

def write_inventory_csv(inventory_data, output_dir, report_datetime_utc):
    """Write inventory data to inventory.csv"""
    file_path = os.path.join(output_dir, INVENTORY_FILE)
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Report Datetime', 'Account ID', 'Account Alias', 'Resource Type', 'Resource ID', 'Region', 
                         'Source Size (GB)', 'S3 Object Count', 'Number of Snapshots', 
                         'First Snapshot Date', 'Latest Snapshot Date', 'Tags', 'S3 Inventory Configs'])
        
        for item in inventory_data:
            # Format S3 Inventory configs for display
            inventory_configs_str = ''
            if item.get('s3_inventory_configs') and item['resource_type'] == 'S3':
                configs = item['s3_inventory_configs']
                if configs:
                    config_summaries = []
                    for cfg in configs:
                        status = 'Enabled' if cfg.get('enabled') else 'Disabled'
                        freq = cfg.get('frequency', 'Unknown')
                        fmt = cfg.get('format', 'Unknown')
                        dest = cfg.get('destination_bucket', 'Unknown')
                        dest_account = cfg.get('destination_account', 'Unknown')
                        versions = cfg.get('included_object_versions', 'Unknown')
                        prefix = cfg.get('prefix', '')
                        fields = cfg.get('optional_fields', [])
                        
                        # Build comprehensive config string
                        config_str = f"ID={cfg.get('id', 'Unknown')}"
                        config_str += f" Status={status}"
                        config_str += f" Freq={freq}"
                        config_str += f" Format={fmt}"
                        config_str += f" DestBucket={dest}"
                        config_str += f" DestAccount={dest_account}"
                        config_str += f" Versions={versions}"
                        if prefix:
                            config_str += f" Prefix={prefix}"
                        if fields:
                            config_str += f" Fields=[{','.join(fields[:5])}{'...' if len(fields) > 5 else ''}]"

                        config_summaries.append(f"[{config_str}]")
                    inventory_configs_str = ' | '.join(config_summaries)
            
            writer.writerow([
                report_datetime_utc,
                item['account_id'],
                account_alias_map.get(item['account_id'], ''),
                item['resource_type'],
                item['resource_id'],
                item['region'],
                format_size_gb(item['source_size']),
                item.get('object_count', ''),
                item['snapshot_count'],
                item['first_snapshot_date'],
                item['latest_snapshot_date'],
                str(item['tags']) if item['tags'] else '',
                inventory_configs_str
            ])

def write_snapshots_csv(snapshot_data, output_dir, report_datetime_utc):
    """Write snapshot data to snapshots.csv"""
    file_path = os.path.join(output_dir, SNAPSHOTS_FILE)
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Report Datetime', 'Account ID', 'Account Alias', 'Resource Type', 'Snapshot ID', 'Region', 
                         'Snapshot Date', 'Estimated Size (GB)', 'Allocated Size (GB)', 'Source Resource ID', 'Tags'])
        
        for item in snapshot_data:
            # Convert full_snapshot_size_bytes to GB if available
            allocated_bytes_gb = ''
            if item.get('full_snapshot_size_bytes') is not None:
                allocated_bytes_gb = format_size_gb(bytes_to_gb(item['full_snapshot_size_bytes']))
            
            writer.writerow([
                report_datetime_utc,
                item['account_id'],
                account_alias_map.get(item['account_id'], ''),
                item['resource_type'],
                item['snapshot_id'],
                item['region'],
                item['snapshot_date'],
                format_size_gb(item['snapshot_size']),
                allocated_bytes_gb,
                item['source_resource_id'],
                str(item['tags']) if item['tags'] else ''
            ])

def write_aws_backup_csv(backup_data, output_dir, report_datetime_utc):
    """Write AWS Backup plan and selection details to backup.csv"""
    if not backup_data:
        verbose_print("No AWS Backup plan/selection data found, skipping backup.csv")
        return
    
    file_path = os.path.join(output_dir, "backup.csv")
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Report Datetime', 'Account ID', 'Account Alias', 'Region', 'Plan ID', 'Plan Name',
            'Selection ID', 'Selection Name', 'Schedule Expression', 'Backup Selection'
        ])
        
        for item in backup_data:
            backup_selection_str = str(item['backup_selection'])

            writer.writerow([
                report_datetime_utc,
                item['account_id'],
                account_alias_map.get(item['account_id'], ''),
                item['region'],
                item['plan_id'],
                item['plan_name'],
                item['selection_id'],
                item['selection_name'],
                item.get('schedule_expression', ''),
                backup_selection_str
            ])

def write_aws_backup_recovery_points_csv(backup_recovery_points, output_dir, report_datetime_utc):
    """Write AWS Backup recovery points to backup_recovery_points.csv"""
    if not backup_recovery_points:
        verbose_print("No AWS Backup recovery point data found, skipping backup_recovery_points.csv")
        return
    
    file_path = os.path.join(output_dir, "backup_recovery_points.csv")
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Report Datetime', 'Account ID', 'Account Alias', 'Region', 'Vault ARN', 'Recovery Point ARN',
            'Resource ARN', 'Resource Type', 'Creation Date', 'Status',
            'Size (GB)', 'Lifecycle', 'Tags',
            'CalculatedLifecycle DeleteAt',
            'CalculatedLifecycle MoveToColdStorageAt',
            'Last Restore Time',
            'Parent Recovery Point Arn',
            'Is Parent',
            'Index Status',
            'Index Status Message',
            'Vault Type'
        ])
        
        for item in backup_recovery_points:
            writer.writerow([
                report_datetime_utc,
                item['account_id'],
                account_alias_map.get(item['account_id'], ''),
                item['region'],
                item['vault_arn'],
                item['recovery_point_arn'],
                item['resource_arn'],
                item['resource_type'],
                item['creation_date'],
                item['status'],
                format_size_gb(item['backup_size_gb']),
                str(item['lifecycle']) if item['lifecycle'] else '',
                str(item['tags']) if item['tags'] else '',
                item.get('calculated_delete_at', ''),
                item.get('calculated_move_to_cold_at', ''),
                item.get('last_restore_time', ''),
                item.get('parent_recovery_point_arn', ''),
                item.get('is_parent', ''),
                item.get('index_status', ''),
                item.get('index_status_message', ''),
                item.get('vault_type', '')
            ])

def write_summary_file(summary_data, output_dir, report_datetime_utc):
    """Write summary data to summary.txt"""
    file_path = os.path.join(output_dir, SUMMARY_FILE)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write("EON CUSTOMER ENVIRONMENT SIZING TOOL SUMMARY\n")
        f.write("===========================================\n")
        # Include tool version information
        try:
            version_info = getattr(sys.modules[__name__], 'TOOL_VERSION', 'dev')
        except Exception:
            version_info = 'dev'
        f.write(f"Tool version: {version_info}\n")
        f.write(f"Report Generated: {report_datetime_utc}\n\n")
        
        f.write(f"Number of AWS Accounts: {summary_data['Accounts']['count']}\n\n")
        
        f.write("RESOURCE SUMMARY\n")
        f.write("-----------------\n")
        f.write(f"EC2 Instances: {summary_data['EC2']['count']} (Total Storage: {format_size_gb(summary_data['EC2']['storage'])} GB)\n")
        f.write(f"RDS Databases: {summary_data['RDS']['count']} (Total Storage: {format_size_gb(summary_data['RDS']['storage'])} GB)\n")
        f.write(f"DynamoDB Tables: {summary_data['DynamoDB']['count']} (Total Storage: {format_size_gb(summary_data['DynamoDB']['storage'])} GB)\n")
        f.write(f"S3 Buckets: {summary_data['S3']['count']} (Objects: {summary_data['S3']['objects']}, Size: {format_size_gb(summary_data['S3']['storage'])} GB)\n")
        if summary_data['S3'].get('inventory_enabled_count', 0) > 0:
            f.write(f"  S3 Inventory: {summary_data['S3']['inventory_enabled_count']} buckets with {summary_data['S3']['total_inventory_configs']} total configurations\n")
        else:
            f.write(f"  S3 Inventory: No S3 Inventory configurations found\n")
        f.write(f"EKS Clusters: {summary_data['EKS']['count']} (Volumes: {summary_data['EKS']['volumes']}, Size: {format_size_gb(summary_data['EKS']['storage'])} GB)\n")
        f.write(f"EFS File Systems: {summary_data['EFS']['count']} (Total Storage: {format_size_gb(summary_data['EFS']['storage'])} GB)\n")
        f.write(f"Redshift Clusters: {summary_data['Redshift']['count']} (Total *Used* Storage: {format_size_gb(summary_data['Redshift']['storage'])} GB)\n")
        f.write(f"  Redshift Total Provisioned: {format_size_gb(summary_data['Redshift']['total_provisioned_storage_gb'])} GB\n")
        f.write(f"  Redshift Total Native Backup Size: {format_size_gb(summary_data['Redshift']['total_backup_size_gb'])} GB\n")
        f.write(f"DocumentDB Databases: {summary_data['DocumentDB']['count']} (Total Storage: {format_size_gb(summary_data['DocumentDB']['storage'])} GB)\n")
        f.write(f"OpenSearch Clusters: {summary_data['OpenSearch']['count']} (Total Storage: {format_size_gb(summary_data['OpenSearch']['storage'])} GB)\n")
        f.write(f"FSx File Systems: {summary_data['FSx']['count']} (Total Storage: {format_size_gb(summary_data['FSx']['storage'])} GB)\n\n")
        
        f.write("SNAPSHOT SUMMARY\n")
        f.write("----------------\n")
        f.write(f"EC2 Snapshots: {summary_data['EC2']['snapshots']}\n")
        f.write(f"RDS Snapshots: {summary_data['RDS']['snapshots']}\n")
        f.write(f"EFS Snapshots: {summary_data['EFS']['snapshots']}\n")
        f.write(f"Redshift Snapshots: {summary_data['Redshift']['snapshots']}\n")
        f.write(f"DocumentDB Snapshots: {summary_data['DocumentDB']['snapshots']}\n")
        f.write(f"OpenSearch Snapshots: {summary_data['OpenSearch']['snapshots']}\n")
        f.write(f"FSx Snapshots: {summary_data['FSx']['snapshots']}\n\n")
        
        services_with_aws_backup = []
        services_with_native_backup = []
        
        # List of services to check for backup storage
        service_list = ['EC2', 'RDS', 'DynamoDB', 'S3', 'EKS', 'EFS', 'Redshift', 'DocumentDB', 'OpenSearch', 'FSx']
        
        for service in service_list:
            if service in summary_data:
                actual_storage = summary_data[service]['storage']
                aws_backup_storage = summary_data[service].get('backup_storage', 0)

                native_backup_storage = 0
                if service == 'Redshift':
                    native_backup_storage = summary_data[service].get('total_backup_size_gb', 0)
                
                if actual_storage > 0 or aws_backup_storage > 0 or native_backup_storage > 0:
                    if native_backup_storage > 0:
                        services_with_native_backup.append(service)
                    
                    if aws_backup_storage > 0:
                        services_with_aws_backup.append(service)

        if services_with_aws_backup:
            f.write(f"Services with AWS Backup enabled: {', '.join(services_with_aws_backup)}\n")
        if services_with_native_backup:
            f.write(f"Services with native backup storage: {', '.join(services_with_native_backup)}\n")
        if not services_with_aws_backup and not services_with_native_backup:
            f.write("No services found with backup storage.\n")
        f.write("\n")
        
        f.write("AWS BACKUP\n")
        f.write("----------\n")
        f.write(f"AWS Backup Configured: {'Yes' if summary_data['AWS_Backup']['configured'] else 'No'}\n")
        f.write(f"Accounts with AWS Backup: {len(backup_accounts_with_config)} out of {summary_data['Accounts']['count']} total accounts\n")
        if backup_accounts_with_errors:
            f.write(f"Accounts with AWS Backup data retrieval errors: {len(backup_accounts_with_errors)}\n")
        if summary_data['AWS_Backup']['configured']:
            f.write(f"AWS Backup Plans: {len(set([item['plan_id'] for item in backup_data]))}\n")
            f.write(f"AWS Backup Selections: {len(backup_data)}\n")
            f.write(f"AWS Backup Recovery Points: {len(backup_recovery_points)}\n")

def check_for_newer_version():
    """Check if a newer version of this tool is available from the remote S3 location."""
    try:
        import requests  # type: ignore
        import re
        remote_url = "https://s3.us-east-1.amazonaws.com/eon-public-b2b628cc-1d96-4fda-8dae-c3b1ad3ea03b/customer-tools/aws_sizing_tool.py"

        try:
            verbose_print("Checking for newer version...")
        except Exception:
            pass

        response = requests.get(remote_url, timeout=10)
        if response.status_code != 200:
            try:
                verbose_print(f"Could not fetch remote version (HTTP {response.status_code})")
            except Exception:
                pass
            return False

        remote_content = response.text
        match = re.search(r'TOOL_VERSION\s*=\s*["\']([^"\']+)["\']', remote_content)
        if not match:
            try:
                verbose_print("Could not parse remote version information.")
            except Exception:
                pass
            return False

        remote_version = match.group(1)
        current_version = getattr(sys.modules[__name__], 'TOOL_VERSION', 'dev')

        try:
            verbose_print(f"Current version: {current_version}")
            verbose_print(f"Remote version: {remote_version}")
        except Exception:
            pass

        if current_version != "dev" and remote_version != "dev":
            if is_newer_version(remote_version, current_version):
                print("\n" + "-" * 60)
                print("  NEWER VERSION AVAILABLE")
                print("-" * 60)
                print(f"  Current version: {current_version}")
                print(f"  Latest version:  {remote_version}")
                print(f"  Download from:   {remote_url}")
                print("  Please update your AWS sizing tool script.")
                print("-" * 60 + "\n")
                return True
            return False
        elif current_version == "dev" and remote_version != "dev":
            print(f"\nNote: You are using a development version. Latest released version is {remote_version}")
            return False
        else:
            try:
                verbose_print("You are using the latest version.")
            except Exception:
                pass
            return False

    except Exception as e:
        # Handle timeouts and network errors gracefully
        try:
            from requests import exceptions as req_exc  # type: ignore
            if isinstance(e, (req_exc.Timeout, req_exc.RequestException)):
                verbose_print(f"Version check failed due to network error: {e}")
                return False
        except Exception:
            pass
        try:
            verbose_print(f"Version check failed: {e}")
        except Exception:
            pass
        return False

def is_newer_version(remote_version, current_version):
    """Compare two version strings in YYYY.MM.DD-hash format and decide if remote is newer."""
    try:
        def parse_version_date(version_str):
            parts = version_str.split('-')[0].split('.')
            if len(parts) == 3:
                year, month, day = map(int, parts)
                return (year, month, day)
            return None

        remote_date = parse_version_date(remote_version)
        current_date = parse_version_date(current_version)

        if remote_date and current_date:
            return remote_date > current_date
        else:
            # Fall back to string inequality if date parsing fails
            return remote_version != current_version
    except (ValueError, AttributeError):
        return remote_version != current_version

def write_error_log(errors_log_data, output_dir, report_datetime_utc):
    """Write errors to errors.log"""
    if errors_log_data:
        file_path = os.path.join(output_dir, ERROR_LOG_FILE)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"Error Log Generated: {report_datetime_utc}\n")
            f.write("======================================\n")
            for error in errors_log_data:
                f.write(f"{error}\n")

def process_account(account, current_account_id, root_account_id, role_name):
    """Process all resources for a single account"""
    account_id = account['Id']
    
    # Determine if we need to assume a role or use current credentials
    credentials = None
    if account_id != current_account_id:
        credentials = assume_role(account_id, current_account_id, root_account_id, role_name)
        if not credentials:
            error_print(f"Could not assume role in account {account_id}, skipping")
            return
    
    # Create the session for this account
    try:
        session = boto3.Session(**credentials) if credentials else boto3.Session()
    except Exception as session_e:
        error_print(f"Failed to create Boto3 session for account {account_id}: {session_e}")
        return

    # Register event handlers on this specific session if logging is enabled
    if aws_log_file_handle:
        try:
            verbose_print(f"Registering log handlers on session for account {account_id}")
            session.events.register('provide-client-params.*.*', log_aws_request)
            session.events.register('after-call.*.*', log_aws_response)
        except Exception as reg_e:
            error_print(f"Failed to register log handlers on session for account {account_id}: {reg_e}", account_id)
            # Continue without logging for this session if registration fails

    # Resolve and cache the account alias (best-effort)
    try:
        if account_id not in account_alias_map:
            alias = get_account_alias(session, account_id)
            account_alias_map[account_id] = alias
            verbose_print(f"Account {account_id} alias resolved to '{alias}'")
    except Exception as alias_e:  # Extra guard; should not raise per get_account_alias impl
        verbose_print(f"Unexpected error caching alias for account {account_id}: {str(alias_e)}")

    # Get list of regions using the account's session
    regions = get_regions(session)

    # Create a function to process a single resource type in a specific region
    def process_resource(resource_func, region):
        try:
            resource_name = resource_func.__name__.replace('get_', '').replace('_resources', '')
            verbose_print(f"Processing {resource_name} in region {region} for account {account_id}")
            return resource_func(account_id, region, session)
        except Exception as e:
            error_print(f"Error processing {resource_func.__name__} in region {region}: {str(e)}", account_id)
            return 0

    # Define the resource collection functions to run
    resource_functions = [
        get_backup_resources,
        get_ec2_resources,
        get_rds_resources,
        get_dynamodb_resources,
        get_eks_resources,
        get_efs_resources,
        get_redshift_resources,
        get_docdb_resources,
        get_opensearch_resources,
        get_fsx_resources
    ]
    
    # Process each resource type in each region in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        # Submit all the jobs
        futures = []
        for region in regions:
            for func in resource_functions:
                futures.append(executor.submit(process_resource, func, region))
        
        # Submit S3 to global executor (created in main) to avoid per-account pool growth
        futures.append(s3_bucket_executor.submit(get_s3_resources, account_id, session))
        
        # Wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                error_print(f"Error in resource collection: {str(e)}", account_id)

def main():
    """Main execution function"""
    global args
    # Banner and version
    print("EON AWS Sizing Tool")
    print("=" * 40)

    args = parse_arguments()

    # Check for a newer version (best-effort, non-blocking)
    check_for_newer_version()
    
    # Create output directory
    output_dir = setup_output_directory()
    print(f"Results will be saved to: {output_dir}")
    try:
        version_info = getattr(sys.modules[__name__], 'TOOL_VERSION', 'dev')
    except Exception:
        version_info = 'dev'
    print(f"Tool version: {version_info}")
    
    # Capture the report generation time in UTC
    report_datetime_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"Report timestamp: {report_datetime_utc}")
    
    if aws_log_file_handle: # Only register if file opened successfully
        verbose_print("Registering Boto3 event handlers on default session for API logging...")
        try:
            boto3.setup_default_session() # Ensures default session context exists
            event_system = boto3.DEFAULT_SESSION.events # Get events from the established default session
            event_system.register('provide-client-params.*.*', log_aws_request)
            event_system.register('after-call.*.*', log_aws_response)
            verbose_print("Default session event handlers registered.")
        except Exception as e:
            error_print(f"Failed to register Boto3 event handlers: {e}")

    # Get current account
    current_account = get_current_account()
    current_account_id = current_account['Account']
    print(f"Current AWS Account: {current_account_id}")
    
    # Determine which accounts to process
    accounts = []
    root_account_id = None
    
    if args.all_accounts:
        print("Getting all accounts in the AWS Organization...")
        root_account_id, org_accounts = get_organization_accounts()
        if org_accounts:
            accounts = org_accounts
            print(f"Found {len(accounts)} accounts in the organization")
        else:
            print("No accounts found or not authorized to access organization data")
            sys.exit(1)
    
    elif args.ou_id:
        print(f"Getting accounts in Organizational Unit: {args.ou_id}")
        root_account_id, ou_accounts = get_ou_accounts(args.ou_id)
        if ou_accounts:
            accounts = ou_accounts
            print(f"Found {len(accounts)} accounts in the specified OU and its children")
        else:
            print("No accounts found in the specified OU or not authorized to access organization data")
            sys.exit(1)
            
    elif args.account_id:
        print(f"Processing single account: {args.account_id}")
        accounts = [{'Id': args.account_id, 'Name': args.account_id}]
    
    elif os.path.exists(args.accounts_file):
        print(f"Reading accounts from file: {args.accounts_file}")
        accounts = get_accounts_from_file(args.accounts_file)
        print(f"Found {len(accounts)} accounts in the file")
    
    else:
        # Default to current account
        print("Processing current account only")
        accounts = [{'Id': current_account_id, 'Name': current_account_id}]
    
    summary['Accounts']['count'] = len(accounts)
    
    print("\nGathering resource data...")

    global s3_bucket_executor
    if s3_bucket_executor is None:
        workers = args.max_workers if args.max_workers and args.max_workers > 0 else 5
        s3_bucket_executor = concurrent.futures.ThreadPoolExecutor(max_workers=workers)
        atexit.register(_shutdown_s3_executor)
    
    for account in accounts:
        print(f"Processing account: {account['Id']}")
        process_account(account, current_account_id, root_account_id, args.role_name)
    
    # Aggregate AWS Backup storage by service type
    if backup_recovery_points:
        print("Aggregating AWS Backup storage by service...")
        aggregate_backup_storage_by_service()
    
    print("\nWriting results...")
    try:
        write_inventory_csv(inventory_data, output_dir, report_datetime_utc)
        write_snapshots_csv(snapshot_data, output_dir, report_datetime_utc)
        if summary['AWS_Backup']['configured']:
            write_aws_backup_csv(backup_data, output_dir, report_datetime_utc)
            write_aws_backup_recovery_points_csv(backup_recovery_points, output_dir, report_datetime_utc)
        write_summary_file(summary, output_dir, report_datetime_utc)
        write_error_log(errors_log, output_dir, report_datetime_utc)
    except Exception as write_e:
        error_print(f"Failed to write output files: {str(write_e)}", print_to_console=True)
    
    # Print full summary from the summary file
    print("\nSummary:")
    summary_file_path = os.path.join(output_dir, SUMMARY_FILE)
    try:
        with open(summary_file_path, 'r', encoding='utf-8') as f:
            print(f.read())
    except Exception as e:
        error_print(f"Error reading summary file: {str(e)}")
    
    print(f"\nResults have been saved to: {output_dir}")
    if errors_log:
        print(f"Warning: {len(errors_log)} errors occurred. See {os.path.join(output_dir, ERROR_LOG_FILE)} for details.")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    main() 
