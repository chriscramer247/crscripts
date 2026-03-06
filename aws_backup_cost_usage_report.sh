#!/bin/bash

# ==============================================================================
# AWS Cost Explorer Report - Snapshots & Backups
# ==============================================================================

# Create a log file with date-time name
LOG_FILE="aws_report_$(date +%Y-%m-%d_%H-%M-%S).txt"
echo "Logging output to: $LOG_FILE"
# Redirect stdout (1) and stderr (2) to both terminal and the log file
exec > >(tee -i "$LOG_FILE") 2>&1

# 1. Credentials Check
# ------------------------------------------------------------------------------

# Get Current Identity info
CURRENT_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
CURRENT_ARN=$(aws sts get-caller-identity --query Arn --output text)

if [ -z "$AWS_DEFAULT_REGION" ]; then
    export AWS_DEFAULT_REGION=us-east-1
fi

# 2. Date Calculation (Cross-Platform)
# ------------------------------------------------------------------------------
if date -v -1d > /dev/null 2>&1; then
    # macOS / BSD
    # Start: 1st of Previous Month
    START_DATE=$(date -v-1m +%Y-%m-01)
    # End: 1st of Current Month (Exclusive for AWS CE)
    END_DATE=$(date +%Y-%m-01)
else
    # Linux / GNU
    START_DATE=$(date -d "$(date +%Y-%m-01) -1 month" +%Y-%m-01)
    END_DATE=$(date +%Y-%m-01)
fi

echo "============================================================"
echo " AWS Cost Report"
echo " Collection Period: Previous Calendar Month"
echo " Date Range:        $START_DATE to $END_DATE"
echo " Connected Account: $CURRENT_ACCOUNT"
echo " Identity ARN:      $CURRENT_ARN"
echo "============================================================"

# Helper Function: Fetch & Filter
# ------------------------------------------------------------------------------
get_cost() {
    TITLE=$1
    SERVICE=$2
    GREP_FILTER=$3 

    echo ""
    echo "[$TITLE]"
    
    # Query Cost Explorer
    # GroupBy USAGE_TYPE AND REGION to get regional breakdown
    
    OUTPUT=$(aws ce get-cost-and-usage \
        --time-period Start=$START_DATE,End=$END_DATE \
        --granularity MONTHLY \
        --metrics "NetUnblendedCost" "UsageQuantity" \
        --group-by Type=DIMENSION,Key=USAGE_TYPE Type=DIMENSION,Key=REGION \
        --filter "{\"Dimensions\": {\"Key\": \"SERVICE\", \"Values\": [\"$SERVICE\"]}}" \
        --output text \
        --query 'ResultsByTime[*].Groups[*].[Keys[0], Keys[1], Metrics.UsageQuantity.Amount, Metrics.NetUnblendedCost.Amount]')

    # Output Format: USAGE_TYPE  REGION  QTY  COST
    
    TOTAL_COST=0
    TOTAL_QTY=0
    MATCH_COUNT=0
    
    # We will accumulate regional data in a temp content variable
    # Format: REGION|COST|QTY
    REGIONAL_DATA=""

    # Using a temporary file to handle reading loop variables properly
    while read -r USAGE_TYPE REGION QTY COST; do
        if [ -z "$USAGE_TYPE" ]; then continue; fi
        
        # Check if UsageType matches our keyword
        if echo "$USAGE_TYPE" | grep -q "$GREP_FILTER"; then
            # Add to totals
            TOTAL_COST=$(awk "BEGIN {print $TOTAL_COST + $COST}")
            TOTAL_QTY=$(awk "BEGIN {print $TOTAL_QTY + $QTY}")
            MATCH_COUNT=$((MATCH_COUNT + 1))
            
            # Append to regional data list
            REGIONAL_DATA="${REGIONAL_DATA}${REGION} ${COST} ${QTY}"$'\n'
        fi
    done <<< "$OUTPUT"
    
    # Print Summary
    printf "  > Total Usage: %.2f GB-Mo (approx)\n" "$TOTAL_QTY"
    printf "  > Total Cost:  $%.2f\n" "$TOTAL_COST"
    
    if [ $MATCH_COUNT -eq 0 ]; then
        echo "  (No matching usage found)"
    else
        # Aggregate by Region using awk
        echo "  > Breakdown by Region:"
        echo "$REGIONAL_DATA" | awk '
            NF {
                region_cost[$1] += $2; 
                region_qty[$1] += $3
            } 
            END {
                for (r in region_cost) {
                    if (region_cost[r] > 0 || region_qty[r] > 0)
                        printf "    - %-15s | Cost: $%6.2f | Usage: %6.2f\n", r, region_cost[r], region_qty[r]
                }
            }' | sort -k4 -rn  # Sort by Cost (column 4 approx, checking field pos)
            # sort -k4 -rn sorts by Cost descending (Cost is field 4 in string "Region | Cost: $X | ...")
    fi
}

# 1] EC2 Other - EBS Snapshots
# Service: "EC2 - Other"
# Keyword: "EBS:SnapshotUsage"
get_cost "1) EC2 EBS Snapshots" "EC2 - Other" "EBS:SnapshotUsage"

# 2] RDS Snapshots (Non-Aurora)
# Service: "Amazon Relational Database Service"
# Keyword: "RDS:SnapshotUsage" OR "RDS:ChargedBackupUsage"
# We use regex for grep: "RDS:.*Snapshot\|RDS:.*Backup"
get_cost "2) RDS Snapshots (Standard)" "Amazon Relational Database Service" "RDS:.*Snapshot\|RDS:.*Backup\|RDS:ChargedBackupUsage"

# 3] Aurora Snapshots
# Service: "Amazon Relational Database Service"
# Keyword: "Aurora:SnapshotUsage" OR "Aurora:BackupUsage"
get_cost "3) Aurora Snapshots" "Amazon Relational Database Service" "Aurora:.*Snapshot\|Aurora:.*Backup"

# 4] DynamoDB PITR
# Service: "Amazon DynamoDB"
# Keyword: "TimedPITRStorage-ByteHrs"
get_cost "4) DynamoDB PITR" "Amazon DynamoDB" "TimedPITRStorage-ByteHrs"

# 5] DynamoDB On-Demand Backup
# Service: "Amazon DynamoDB"
# Keyword: "GuaranteedBackupUsage"
get_cost "5) DynamoDB On-Demand" "Amazon DynamoDB" "GuaranteedBackupUsage"

# 6] Redshift Snapshots
# Service: "Amazon Redshift"
# Keyword: "Redshift:.*Snapshot" OR "Redshift:.*Backup"
get_cost "6) Redshift Snapshots" "Amazon Redshift" "Redshift:.*Snapshot\|Redshift:.*Backup"

# 7] S3 Storage (All Classes)
# Service: "Amazon Simple Storage Service"
# Keyword: Matches "TimedStorage-ByteHrs", "TimedStorage-*IA*-ByteHrs", "TimedStorage-*INT*-ByteHrs", "Glacier*", etc.
get_cost "7) S3 Storage" "Amazon Simple Storage Service" "Storage.*ByteHrs"

echo "   - Breakdown:"
aws ce get-cost-and-usage \
    --time-period Start=$START_DATE,End=$END_DATE \
    --granularity MONTHLY \
    --metrics "NetUnblendedCost" "UsageQuantity" \
    --group-by Type=DIMENSION,Key=USAGE_TYPE \
    --filter "{\"Dimensions\": {\"Key\": \"SERVICE\", \"Values\": [\"Amazon Simple Storage Service\"]}}" \
    --output text \
    --query 'ResultsByTime[*].Groups[*].[Keys[0], Metrics.UsageQuantity.Amount, Metrics.NetUnblendedCost.Amount]' | \
while read -r UTYPE QTY COST; do
    if echo "$UTYPE" | grep -q "Storage.*ByteHrs"; then
        printf "     * %-40s | %-10s | $%-10s\n" "$UTYPE" "$QTY" "$COST"
    fi
done

# 8] EFS Backups
# Service: "Amazon Elastic File System"
# Keyword: "BackupUsage"
get_cost "8) EFS Backups" "Amazon Elastic File System" "Backup"

# 9] AWS Backup Service
# Service: "AWS Backup"
# Show Everything under this service, broken down by type if possible.
# Since AWS Backup service usage types aren't always standard named by resource, 
# we'll list the raw breakdown for visibility.
echo ""
echo "[9] AWS Backup Service (Direct Charges)"
echo "------------------------------------------------------------"
aws ce get-cost-and-usage \
    --time-period Start=$START_DATE,End=$END_DATE \
    --granularity MONTHLY \
    --metrics "NetUnblendedCost" "UsageQuantity" \
    --group-by Type=DIMENSION,Key=USAGE_TYPE \
    --filter "{\"Dimensions\": {\"Key\": \"SERVICE\", \"Values\": [\"AWS Backup\"]}}" \
    --output text \
    --query 'ResultsByTime[*].Groups[*].[Keys[0], Metrics.UsageQuantity.Amount, Metrics.NetUnblendedCost.Amount]' | \
while read -r UTYPE QTY COST; do
    printf "  - %-40s | Qty: %-10s | Cost: $%-10s\n" "$UTYPE" "$QTY" "$COST"
done

echo ""
echo "Note: Resource-specific backup costs (e.g. S3, EBS) usually appear"
echo "under their respective services (Sections 1-4) unless specifically"
echo "billed as 'AWS Backup' management fees or cross-region data transfer."
echo "============================================================"

# 10] DynamoDB Table Usage
# Service: "Amazon DynamoDB"
# Keyword: "TimedStorage-ByteHrs" (Standard Table Usage)
get_cost "10) DynamoDB Table Usage" "Amazon DynamoDB" "TimedStorage-ByteHrs"

echo "  > Table List (Active Region: $AWS_DEFAULT_REGION):"
TABLE_LIST=$(aws dynamodb list-tables --query "TableNames[]" --output text | tr '\t' '\n')

if [ -z "$TABLE_LIST" ]; then
    echo "    (No tables found in this region)"
else
    printf "    %-40s | %10s\n" "Table Name" "Size (GB)"
    printf "    %-40s | %10s\n" "----------------------------------------" "----------"
    
    for TABLE in $TABLE_LIST; do
        # Fetch size in bytes
        SIZE_BYTES=$(aws dynamodb describe-table --table-name "$TABLE" --query "Table.TableSizeBytes" --output text 2>/dev/null)
        
        if [ -n "$SIZE_BYTES" ] && [ "$SIZE_BYTES" != "None" ]; then
            # Convert to GB
            SIZE_GB=$(awk "BEGIN {printf \"%.4f\", $SIZE_BYTES/1024/1024/1024}")
            printf "    %-40s | %10s\n" "$TABLE" "$SIZE_GB"
        else
            printf "    %-40s | %10s\n" "$TABLE" "Unknown"
        fi
    done
fi
echo "============================================================"
