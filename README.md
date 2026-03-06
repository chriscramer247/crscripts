# crscripts

**STEP 1:** Run aws_sizing_tool.py

AWS Environment Sizing
=================================================
Prerequisites (these already exist in AWS CloudShell):
•	Python 3.6 or higher
•	AWS CLI configured with appropriate permissions
•	Required Python packages: boto3, botocore

Instructions:
1. Download the AWS sizing tool:
 wget https://s3.us-east-1.amazonaws.com/eon-public-b2b628cc-1d96-4fda-8dae-c3b1ad3ea03b/customer-tools/aws_sizing_tool.py
2. Install python and the prerequisite packages with pip install boto3 botocore (These already exist in AWS Cloudshell so no need for this step there)
3. Run the sizing tool with: python aws_sizing_tool.py with ReadOnly permissions
    For help -      python aws_sizing_tool.py --help
4. Please send us the CSV files that this tool creates

**STEP 2:** Run aws_backup_cost_usage_report.sh

Run the script aws_backup_cost_usage_report.sh with **STS Access to their Master billing account** 
