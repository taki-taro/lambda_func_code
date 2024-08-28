import base64
import logging
import json
import zlib
import datetime
import time
import os
from datetime import datetime, timezone, timedelta
import boto3
from botocore.exceptions import ClientError


# logger初期化
logger = logging.getLogger()
logger.setLevel(logging.INFO)


#ハンドラー
def lambda_handler(event, context):
    logger.info("LOAD Function: " + context.function_name)

    time.sleep(30)

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

    #Lambdaサブスクリプションフィルターから連携された情報からデータ取得
    account_id = cwlogs_raw_data_json['owner']
    log_group_name = cwlogs_raw_data_json['logGroup']
    log_stream_name = cwlogs_raw_data_json['logStream']
    log_timestamp = convert_time(int(cwlogs_raw_data_json['logEvents'][0]['timestamp']))
    log_message = cwlogs_raw_data_json['logEvents'][0]['message']
    target_timestamp = cwlogs_raw_data_json['logEvents'][0]['timestamp']

    #DEBUG
    print(target_timestamp)
    print("log_message", log_message, sep=':')

    #ログストリーム内のログデータを取得する
    logs_with_timestamp = get_logs_around_with_ids(log_group_name, log_stream_name)

    #timestampを基に前後のログを取得する
    before = os.environ['GET_LOG_BEFORE']
    after = os.environ['GET_LOG_AFTER']
    context_logs = get_logs_by_timestamp(logs_with_timestamp, target_timestamp, before, after)

    #SNS連携&メール送信
    send_mail(account_id, log_group_name, log_stream_name, log_timestamp, log_message, context_logs)

    logger.info("END Function: " + context.function_name)


# CloudWatchLogs LogsStream内の全てのtimestampとmessageのペアを返す関数
def get_logs_around_with_ids(log_group_name, log_stream_name):
    logs_client = boto3.client('logs')
    response = logs_client.get_log_events(
        logGroupName=log_group_name,
        logStreamName=log_stream_name,
        limit=1000
    )

    #DEBUG
    print(json.dumps(response, indent=2))
    
    logs_with_timestamp = [{'message': event['message'], 'timestamp': event['timestamp']} for event in response['events']]
    
    #DEBUG
    print(json.dumps(logs_with_timestamp, indent=2))
    
    return logs_with_timestamp


# 取得したログのリストから指定したtimestampを持つログを探して、その前後のログを抽出してリストに格納する関数
def get_logs_by_timestamp(logs_with_timestamp, target_timestamp, before, after):
    for i, log in enumerate(logs_with_timestamp):
        if log['timestamp'] == target_timestamp:
            # 負の値は考えられないためmaxで抑制する
            start_index = max(0, i - int(before))
            end_index = min(len(logs_with_timestamp), i + int(after) + 1)
            return [log['message'] for log in logs_with_timestamp[start_index:end_index]]
    return []


# タイムスタンプ時刻変換用関数
def convert_time(raw_timestamp):
    # UNIXミリ秒から秒への変換
    timestamp_sec = raw_timestamp / 1000
    dt = datetime.fromtimestamp(timestamp_sec, timezone(timedelta(hours=9)))
    # ISO:8601形式に変換
    timestamp_jst = dt.isoformat()

    return timestamp_jst


# SNS連携用関数
def send_mail(owner, loggroup, logstream, logtimestamp, message, expand_mesages):
    sns_client = boto3.client('sns')
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']
    messages = expand_mesages

    sns_client.publish (
        TopicArn = sns_topic_arn,
        Subject = "ログ監視アラーム",
        Message =(
            "ログ グループ名: " + loggroup + "\n" +
            "ログ ストリーム名: " + logstream + "\n" +
            "ログ 検知日時(JST): " + logtimestamp + "\n" +
            "ログ 内容: " + message + "\n" +
            "エラーログの直前のログ:\n" + 
            "\n".join(messages) + "\n"
        )
    )