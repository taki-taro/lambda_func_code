import base64
import logging
import json
import gzip
import datetime
import os
from datetime import datetime, timezone, timedelta
import boto3
from botocore.exceptions import ClientError

# Setting env
envs = {
    "799658447888": "YAMAZAKI's AWS Account"
}

# log group mapping
loggroups = {
    "LogAlarmTest_001": "テスト用ログ００１",
    "LogAlarmTest_002": "テスト用ログ００２",
    "LogAlarmTest_003": "テスト用ログ００３"
}

# convert time stamp
def convert_time(raw_timestamp):
    timestamp_sec = raw_timestamp / 1000
    dt = datetime.fromtimestamp(timestamp_sec, timezone(timedelta(hours=9)))
    timestamp_jst = dt.isoformat()
    return timestamp_jst

# send mail
def send_mail(owner, loggroup, logstream, logtimestamp, message):
    sns_client = boto3.client('sns')
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']

    sns_client.publish (
        TopicArn = sns_topic_arn,
        Subject = "【" + envs[owner] + "】" + loggroups[loggroup] + "で異常を検知!!!",
        Message = (
            "========================================================================================" + "\n" +
            "AWSアカウントID: " + owner + "\n" +
            "ロググループ名: " + loggroup + "\n" +
            "ログストリーム名: " + logstream + "\n" +
            "ロググループ検知日時(JST): " + logtimestamp + "\n" +
            "ログ内容: " + message + "\n" +
            "========================================================================================"
        )
    )

# logging init
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# main
def lambda_handler(event, context):
    logger.info("LOAD Function: " + context.function_name)

    # read cloudwatch data
    cwlogs_data = json.loads(gzip.decompress(base64.b64decode(event['awslogs']['data'])))

    logger.info("EVENT: " + json.dumps(cwlogs_data))

    account_id = cwlogs_data['owner']
    loggroup_name = cwlogs_data['logGroup']
    logstream_name = cwlogs_data['logStream']
    timestamp = convert_time(int(cwlogs_data['logEvents'][0]['timestamp']))
    message = cwlogs_data['logEvents'][0]['message']

    # send alarm mail
    send_mail(account_id, loggroup_name, logstream_name, timestamp, message)
    
    logger.info("END Function: " + context.function_name)