#!/bin/bash
# Treez + METRC Data Sync - Lambda + EventBridge Setup Script
# Deploys automated daily sync at 11:00 PM PST

set -e

# Configuration
FUNCTION_NAME="treez-metrc-sync"
ROLE_NAME="treez-metrc-sync-role"
RULE_NAME="treez-metrc-daily-sync"
REGION="us-west-1"
RUNTIME="python3.11"
HANDLER="lambda_function.lambda_handler"
TIMEOUT=300
MEMORY_SIZE=512

# Aurora VPC Configuration (from chapters-data-cluster)
VPC_ID="vpc-0a1b2c3d4e5f6g7h8"  # Update with actual VPC ID
SUBNET_IDS=""  # Will be populated below
SECURITY_GROUP_ID=""  # Will be populated below

echo "=========================================="
echo "Treez + METRC Sync Lambda Deployment"
echo "=========================================="

# Check AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI is not installed"
    exit 1
fi

# Check credentials
echo "Checking AWS credentials..."
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null)
if [ -z "$AWS_ACCOUNT_ID" ]; then
    echo "Error: AWS credentials not configured"
    exit 1
fi
echo "Using AWS Account: $AWS_ACCOUNT_ID"

# Get VPC configuration for Aurora
echo ""
echo "Fetching VPC configuration for Aurora access..."

# Find the VPC where Aurora is deployed
AURORA_VPC=$(aws rds describe-db-clusters \
    --db-cluster-identifier chapters-data-cluster \
    --region $REGION \
    --query 'DBClusters[0].DBSubnetGroup' \
    --output text 2>/dev/null || echo "")

if [ -n "$AURORA_VPC" ]; then
    # Get subnet IDs from the DB subnet group
    SUBNET_IDS=$(aws rds describe-db-subnet-groups \
        --db-subnet-group-name "$AURORA_VPC" \
        --region $REGION \
        --query 'DBSubnetGroups[0].Subnets[*].SubnetIdentifier' \
        --output text 2>/dev/null | tr '\t' ',')

    # Get VPC ID from first subnet
    FIRST_SUBNET=$(echo $SUBNET_IDS | cut -d',' -f1)
    VPC_ID=$(aws ec2 describe-subnets \
        --subnet-ids $FIRST_SUBNET \
        --region $REGION \
        --query 'Subnets[0].VpcId' \
        --output text 2>/dev/null)

    echo "Found Aurora VPC: $VPC_ID"
    echo "Found Subnets: $SUBNET_IDS"
fi

# Create or get security group for Lambda
echo ""
echo "Setting up security group for Lambda..."

SECURITY_GROUP_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=treez-sync-lambda-sg" "Name=vpc-id,Values=$VPC_ID" \
    --region $REGION \
    --query 'SecurityGroups[0].GroupId' \
    --output text 2>/dev/null || echo "None")

if [ "$SECURITY_GROUP_ID" == "None" ] || [ -z "$SECURITY_GROUP_ID" ]; then
    echo "Creating security group..."
    SECURITY_GROUP_ID=$(aws ec2 create-security-group \
        --group-name "treez-sync-lambda-sg" \
        --description "Security group for Treez/METRC sync Lambda" \
        --vpc-id $VPC_ID \
        --region $REGION \
        --query 'GroupId' \
        --output text)

    # Allow outbound HTTPS (for Treez/METRC APIs)
    aws ec2 authorize-security-group-egress \
        --group-id $SECURITY_GROUP_ID \
        --protocol tcp \
        --port 443 \
        --cidr 0.0.0.0/0 \
        --region $REGION 2>/dev/null || true

    # Allow outbound PostgreSQL (for Aurora)
    aws ec2 authorize-security-group-egress \
        --group-id $SECURITY_GROUP_ID \
        --protocol tcp \
        --port 5432 \
        --cidr 0.0.0.0/0 \
        --region $REGION 2>/dev/null || true

    echo "Created security group: $SECURITY_GROUP_ID"
else
    echo "Using existing security group: $SECURITY_GROUP_ID"
fi

# Create IAM role for Lambda
echo ""
echo "Setting up IAM role..."

TRUST_POLICY='{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}'

# Check if role exists
ROLE_ARN=$(aws iam get-role --role-name $ROLE_NAME --query 'Role.Arn' --output text 2>/dev/null || echo "")

if [ -z "$ROLE_ARN" ]; then
    echo "Creating IAM role: $ROLE_NAME"
    ROLE_ARN=$(aws iam create-role \
        --role-name $ROLE_NAME \
        --assume-role-policy-document "$TRUST_POLICY" \
        --query 'Role.Arn' \
        --output text)

    # Attach basic Lambda execution policy
    aws iam attach-role-policy \
        --role-name $ROLE_NAME \
        --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

    # Attach VPC access policy for Aurora connectivity
    aws iam attach-role-policy \
        --role-name $ROLE_NAME \
        --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole

    # Create custom policy for Secrets Manager access (for API keys)
    SECRETS_POLICY='{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "secretsmanager:GetSecretValue"
                ],
                "Resource": "arn:aws:secretsmanager:'$REGION':'$AWS_ACCOUNT_ID':secret:treez-sync/*"
            }
        ]
    }'

    aws iam put-role-policy \
        --role-name $ROLE_NAME \
        --policy-name "treez-sync-secrets-access" \
        --policy-document "$SECRETS_POLICY"

    echo "Created role: $ROLE_ARN"
    echo "Waiting for role to propagate..."
    sleep 10
else
    echo "Using existing role: $ROLE_ARN"
fi

# Package Lambda function
echo ""
echo "Packaging Lambda function..."

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"
PACKAGE_FILE="$SCRIPT_DIR/lambda_package.zip"

# Clean up previous builds
rm -rf "$BUILD_DIR"
rm -f "$PACKAGE_FILE"
mkdir -p "$BUILD_DIR"

# Install dependencies
echo "Installing dependencies..."
pip install -r "$SCRIPT_DIR/requirements.txt" -t "$BUILD_DIR" --quiet

# Copy Lambda function
cp "$SCRIPT_DIR/lambda_function.py" "$BUILD_DIR/"

# Create zip package
echo "Creating deployment package..."
cd "$BUILD_DIR"
zip -r "$PACKAGE_FILE" . -q
cd "$SCRIPT_DIR"

PACKAGE_SIZE=$(du -h "$PACKAGE_FILE" | cut -f1)
echo "Package created: $PACKAGE_FILE ($PACKAGE_SIZE)"

# Deploy or update Lambda function
echo ""
echo "Deploying Lambda function..."

# Check if function exists
FUNCTION_EXISTS=$(aws lambda get-function --function-name $FUNCTION_NAME --region $REGION 2>/dev/null && echo "yes" || echo "no")

if [ "$FUNCTION_EXISTS" == "yes" ]; then
    echo "Updating existing function..."
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --zip-file "fileb://$PACKAGE_FILE" \
        --region $REGION \
        --output text > /dev/null

    # Update configuration
    aws lambda update-function-configuration \
        --function-name $FUNCTION_NAME \
        --timeout $TIMEOUT \
        --memory-size $MEMORY_SIZE \
        --vpc-config "SubnetIds=$SUBNET_IDS,SecurityGroupIds=$SECURITY_GROUP_ID" \
        --region $REGION \
        --output text > /dev/null 2>/dev/null || true
else
    echo "Creating new function..."
    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --runtime $RUNTIME \
        --role $ROLE_ARN \
        --handler $HANDLER \
        --timeout $TIMEOUT \
        --memory-size $MEMORY_SIZE \
        --zip-file "fileb://$PACKAGE_FILE" \
        --vpc-config "SubnetIds=$SUBNET_IDS,SecurityGroupIds=$SECURITY_GROUP_ID" \
        --region $REGION \
        --output text > /dev/null
fi

FUNCTION_ARN=$(aws lambda get-function --function-name $FUNCTION_NAME --region $REGION --query 'Configuration.FunctionArn' --output text)
echo "Lambda deployed: $FUNCTION_ARN"

# Store secrets in Secrets Manager
echo ""
echo "Setting up Secrets Manager..."

# Check if secrets exist, create if not
SECRET_NAME="treez-sync/api-keys"
SECRET_EXISTS=$(aws secretsmanager describe-secret --secret-id $SECRET_NAME --region $REGION 2>/dev/null && echo "yes" || echo "no")

if [ "$SECRET_EXISTS" == "no" ]; then
    echo "Creating secrets placeholder in Secrets Manager..."
    echo "NOTE: You'll need to update these with your actual API keys"

    aws secretsmanager create-secret \
        --name $SECRET_NAME \
        --description "API keys for Treez and METRC sync" \
        --secret-string '{
            "TREEZ_API_KEY": "YOUR_TREEZ_API_KEY",
            "TREEZ_CLIENT_ID": "chapters_sync",
            "TREEZ_DISPENSARY": "barbarycoast",
            "METRC_API_KEY": "YOUR_METRC_API_KEY",
            "METRC_USER_KEY": "YOUR_METRC_USER_KEY",
            "DATABASE_URL": "YOUR_DATABASE_URL"
        }' \
        --region $REGION \
        --output text > /dev/null

    echo "Created secret: $SECRET_NAME"
    echo ""
    echo "⚠️  IMPORTANT: Update the secret with your actual credentials:"
    echo "   aws secretsmanager update-secret --secret-id $SECRET_NAME --region $REGION --secret-string '{...}'"
else
    echo "Secret already exists: $SECRET_NAME"
fi

# Create EventBridge rule for daily execution at 11 PM PST
echo ""
echo "Setting up EventBridge schedule..."

# 11 PM PST = 7 AM UTC (PST is UTC-8)
# During PDT (daylight saving), 11 PM PDT = 6 AM UTC
SCHEDULE_EXPRESSION="cron(0 7 * * ? *)"

# Check if rule exists
RULE_EXISTS=$(aws events describe-rule --name $RULE_NAME --region $REGION 2>/dev/null && echo "yes" || echo "no")

if [ "$RULE_EXISTS" == "yes" ]; then
    echo "Updating existing EventBridge rule..."
    aws events put-rule \
        --name $RULE_NAME \
        --schedule-expression "$SCHEDULE_EXPRESSION" \
        --state ENABLED \
        --description "Daily Treez/METRC data sync at 11 PM PST" \
        --region $REGION \
        --output text > /dev/null
else
    echo "Creating EventBridge rule..."
    aws events put-rule \
        --name $RULE_NAME \
        --schedule-expression "$SCHEDULE_EXPRESSION" \
        --state ENABLED \
        --description "Daily Treez/METRC data sync at 11 PM PST" \
        --region $REGION \
        --output text > /dev/null
fi

# Add Lambda as target
echo "Adding Lambda as EventBridge target..."
aws events put-targets \
    --rule $RULE_NAME \
    --targets "Id=treez-sync-target,Arn=$FUNCTION_ARN" \
    --region $REGION \
    --output text > /dev/null

# Grant EventBridge permission to invoke Lambda
echo "Granting EventBridge permission to invoke Lambda..."
aws lambda add-permission \
    --function-name $FUNCTION_NAME \
    --statement-id "eventbridge-invoke" \
    --action "lambda:InvokeFunction" \
    --principal events.amazonaws.com \
    --source-arn "arn:aws:events:$REGION:$AWS_ACCOUNT_ID:rule/$RULE_NAME" \
    --region $REGION \
    --output text > /dev/null 2>/dev/null || true

RULE_ARN=$(aws events describe-rule --name $RULE_NAME --region $REGION --query 'Arn' --output text)
echo "EventBridge rule configured: $RULE_ARN"

# Clean up build artifacts
echo ""
echo "Cleaning up..."
rm -rf "$BUILD_DIR"

# Summary
echo ""
echo "=========================================="
echo "Deployment Complete!"
echo "=========================================="
echo ""
echo "Lambda Function: $FUNCTION_NAME"
echo "  ARN: $FUNCTION_ARN"
echo "  Runtime: $RUNTIME"
echo "  Memory: ${MEMORY_SIZE}MB"
echo "  Timeout: ${TIMEOUT}s"
echo ""
echo "EventBridge Rule: $RULE_NAME"
echo "  Schedule: Daily at 11:00 PM PST (7:00 AM UTC)"
echo "  ARN: $RULE_ARN"
echo ""
echo "Secrets Manager: $SECRET_NAME"
echo ""
echo "=========================================="
echo "Next Steps:"
echo "=========================================="
echo ""
echo "1. Update API keys in Secrets Manager:"
echo "   aws secretsmanager update-secret \\"
echo "     --secret-id $SECRET_NAME \\"
echo "     --region $REGION \\"
echo "     --secret-string '{"
echo "       \"TREEZ_API_KEY\": \"your-treez-key\","
echo "       \"TREEZ_CLIENT_ID\": \"chapters_sync\","
echo "       \"TREEZ_DISPENSARY\": \"barbarycoast\","
echo "       \"METRC_API_KEY\": \"your-metrc-api-key\","
echo "       \"METRC_USER_KEY\": \"your-metrc-user-key\","
echo "       \"DATABASE_URL\": \"postgresql://...\"
echo "     }'"
echo ""
echo "2. Test the Lambda function manually:"
echo "   aws lambda invoke \\"
echo "     --function-name $FUNCTION_NAME \\"
echo "     --region $REGION \\"
echo "     --payload '{}' \\"
echo "     response.json && cat response.json"
echo ""
echo "3. Check CloudWatch logs:"
echo "   aws logs tail /aws/lambda/$FUNCTION_NAME --region $REGION --follow"
echo ""
