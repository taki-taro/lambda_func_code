import base64
import logging
import json
import zlib
import datetime
import os
from datetime import datetime, timezone, timedelta
import boto3
from botocore.exceptions import ClientError

# タイムスタンプ時刻変換用関数
def convert_time(raw_timestamp):
    # UNIXミリ秒から秒への変換
    timestamp_sec = raw_timestamp / 1000
    dt = datetime.fromtimestamp(timestamp_sec, timezone(timedelta(hours=9)))
    # ISO:8601形式に変換
    timestamp_jst = dt.isoformat()

    return timestamp_jst

# SNS連携用関数
def send_mail(owner, loggroup, logstream, logtimestamp, message):
    sns_client = boto3.client('sns')
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']

    sns_client.publish (
        TopicArn = sns_topic_arn,
        Subject = "【799658447888】ログ監視アラーム",
        Message =(
            "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + "\n" +
            "AWSアカウントID: " + owner + "\n" +
            "ログ グループ名: " + loggroup + "\n" +
            "ログ ストリーム名: " + logstream + "\n" +
            "ログ 検知日時(JST): " + logtimestamp + "\n" +
            "ログ 内容: " + message + "\n" +
            "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
        )
    )

# logger初期化
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#ハンドラー
def lambda_handler(event, context):
    logger.info("LOAD Function: " + context.function_name)

    #DEBUG
    print("event", event, sep=':')

    #CloudWatchLogsから渡される値は圧縮エンコードされているためデコードして解凍
    cwlogs_raw_data = zlib.decompress(base64.b64decode(event['awslogs']['data']), 16+zlib.MAX_WBITS)
    
    #DEBUG
    print("cwlogs_raw_data", cwlogs_raw_data, sep=':')
    
    #JSON読み込み
    cwlogs_raw_data_json = json.loads(cwlogs_raw_data)

    #DEBUG
    print("cwlogs_raw_data_json", cwlogs_raw_data_json, sep=':')

    #データ格納
    account_id = cwlogs_raw_data_json['owner']
    log_group_name = cwlogs_raw_data_json['logGroup']
    log_stream_name = cwlogs_raw_data_json['logStream']
    log_timestamp = convert_time(int(cwlogs_raw_data_json['logEvents'][0]['timestamp']))
    log_message = cwlogs_raw_data_json['logEvents'][0]['message']

    #DEBUG
    print("log_message", log_message, sep=':')

    #SNS連携&メール送信
    send_mail(account_id, log_group_name, log_stream_name, log_timestamp, log_message)

    logger.info("END Function: " + context.function_name)