"""AWS Lambda that checks for Combiner and Processor execution errors.

Checks quarantine directories and error logs for files that could not be processed.
If any errors are found:
1. Crafts text file to submit to pending_jobs_queue to restart Generate processing
for quarantined files.
2. Removes error_logs and quarantined files.
3. Logs errors and send notification upon failure.
"""

# Standard imports
import datetime
import glob
import json
import logging
import os
import pathlib
import random
import sys
import zipfile

# Third-party imports
import boto3
import botocore
import requests

# Constants
DATA_DIR = pathlib.Path("/mnt/data/")
DATASET_DICT = {
    "AQUA_MODIS": "aqua",
    "TERRA_MODIS": "terra",
    "SNPP_VIIRS": "viirs"
}
GET_FILE_URL = "https://oceandata.sci.gsfc.nasa.gov/cgi/getfile"
PROC_DICT = {
    "A": "AQUA_MODIS",
    "T": "TERRA_MODIS",
    "V": "SNPP_VIIRS"
}
DOWNLOAD_DICT = {
    "AQUA_MODIS": "MODIS_AQUA_L2_SST_OBPG",
    "TERRA_MODIS": "MODIS_TERRA_L2_SST_OBPG",
    "SNPP_VIIRS": "VIIRS_L2_SST_OBPG"
}
SEARCH_URL = "https://oceandata.sci.gsfc.nasa.gov/api/file_search"
TOPIC_STRING = "batch-job-failure"

# Functions
def error_checker_handler(event, context):
    """Handles events from EventBridge and checks for Generate workflow errors."""
    
    logger = get_logger()
    
    txt_dict = {
        "aqua" : [],
        "terra": [],
        "viirs": []
    }
    
    # Find combiner errors
    combiner_file_list, combiner_error_logs = check_combiner()
    if len(combiner_file_list) > 0: 
        logger.info("Located quarantined combiner files in the EFS:")
        for file in combiner_file_list: logger.info(file)
    else: 
        logger.info("No quarantined combiner files were located in the EFS.")
    
    # Find processor errors
    processor_file_list, processor_error_logs = check_processor()
    if len(processor_file_list) > 0: 
        logger.info("Located quarantined processor files in the EFS:")
        for file in processor_file_list: logger.info(file)
    else: 
        logger.info("No quarantined processor files were located in the EFS.")
    
    # If there are no quarantine files, exit
    if len(combiner_file_list) == 0 and len(processor_file_list) == 0:
        logger.info("No combiner or processor error files to process.")
        logger.info("Exit.")
        return
    
    # Search for combiner files in OBPG
    combiner_error_list = search_combiner(combiner_file_list, txt_dict, logger)
    
    # Search for processor files in OBPG
    processor_error_list = search_processor(processor_file_list, txt_dict, logger)
    
    # Create txt files
    txt_list = create_txt_files(txt_dict, logger)
    
    # Upload txt files to S3 bucket
    s3_error = None
    try:
        upload_txt(event["prefix"], txt_list, logger)
        success = True
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error - {e}")
        success = False
        s3_error = e
        
    # Check for success and delete files, publish to pending jobs
    sqs_error = None
    if success:
        zipped = archive_files(combiner_file_list, combiner_error_logs, processor_file_list, processor_error_logs, logger)
        try:
            upload_archive(event["prefix"], zipped, logger)
        except botocore.exceptions.ClientError as e:
            logger.error(e)
            s3_error = e
        logger.info("Archived and removed any quarantined files and error logs from the EFS.")
        try:
            publish_to_pending(txt_list, event["prefix"], event["account"], event["region"], logger)
        except botocore.exceptions.ClientError as e:
            logger.error(f"Error - {e}")
            sqs_error = e
            
    # Report any errors
    if len(combiner_error_list) != 0 or len(processor_error_list) != 0 or s3_error or sqs_error:
        report_errors(event["prefix"], combiner_error_list, processor_error_list, s3_error, sqs_error, logger)
        sys.exit(1)
        
    # Remove /tmp/generate txt files
    remove_tmp(txt_list)
    logger.info("Removed temporary txt files.")
    
def get_logger():
    """Return a formatted logger object."""
    
    # Remove AWS Lambda logger
    logger = logging.getLogger()
    for handler in logger.handlers:
        logger.removeHandler(handler)
    
    # Create a Logger object and set log level
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Create a handler to console and set level
    console_handler = logging.StreamHandler()

    # Create a formatter and add it to the handler
    console_format = logging.Formatter("%(module)s - %(levelname)s : %(message)s")
    console_handler.setFormatter(console_format)

    # Add handlers to logger
    logger.addHandler(console_handler)

    # Return logger
    return logger

def check_combiner():
    """Locate any files that produced combiner processing errors.
    
    Return Tuple of list of files and corresponding error logs.
    """
    
    try:
        with os.scandir(DATA_DIR.joinpath("processor", "input", "quarantine")) as entries:
            file_list = [pathlib.Path(entry) for entry in entries]
    except FileNotFoundError:
        file_list = []
    
    with os.scandir(DATA_DIR.joinpath("processor", "input")) as entries:
        error_log = [pathlib.Path(entry) for entry in entries if "ghrsst_error_log_archive" in entry.name]
    
    return file_list, error_log

def check_processor():
    """Locate any files that produced processor processing errors.
    
    Return Tuple of list of files and corresponding error logs.
    """
    
    try:
        with os.scandir(DATA_DIR.joinpath("processor", "scratch", "quarantine")) as entries:
            file_list = [pathlib.Path(entry) for entry in entries]
    except FileNotFoundError:
        file_list = []
    
    with os.scandir(DATA_DIR.joinpath("processor", "logs", "error_logs")) as entries:
        error_log = [pathlib.Path(entry) for entry in entries]
    
    return file_list, error_log

def search_combiner(file_list, txt_dict, logger):
    """Search OBPG for file list and populate txt dictionary.
    
    Return list of files where error occured.
    """
    
    errors = []
    for file in file_list:
        if "LAC_GSST" in file.name or "SNPP_GSST" in file.name: continue   # Skip failed combined file
        timestamp = file.name.split('.')[1].replace("T", "")
        file_name = file.name.split('.')[0]
        if "NRT" in file.name:
            file_type = '.'.join([file.name.split('.')[3], file.name.split('.')[4]])
        else:
            file_type = file.name.split('.')[3]
        response = query_obpg(timestamp, file_name, file_type)
        response = response.split('\n')
        dataset = DATASET_DICT[file_name]
        
        # Ensure that exact combiner file is retrieved for resubmission
        if response[0] == "No Results Found":
            errors.append(file.name)
            logger.error(f"Could not locate: {file.name} in CMR.")
        elif len(response) == 1:
            logger.info(f"Located: {file.name}.")
            txt_dict[dataset].append([f"{GET_FILE_URL}/{response[0].split(' ')[2]}", response[0].split(' ')[0]])
        else:
            logger.info(f"Located multiple: {file.name} in CMR.")
            logger.info(f"CMR response: {response}.")
            found = False
            for element in response:
                response_file = element.split(' ')[2]
                if file.name == response_file:
                    txt_dict[dataset].append([f"{GET_FILE_URL}/{response_file}", element.split(' ')[0]])
                    logger.info(f"Selected from CMR response: {response_file}")
                    found = True
            # Check for quicklook
            if not found:
                for element in response:
                    nrt_file = f"{file.name[:-3]}.NRT.nc"
                    response_file = element.split(' ')[2]
                    if nrt_file == response_file:
                        txt_dict[dataset].append([f"{GET_FILE_URL}/{response_file}", element.split(' ')[0]])
                        logger.info(f"Selected from CMR response: {response_file}")
                        found = True
            if not found:
                logger.info(f"Could not select a file for: {file.name} from CMR response.")
                errors.append(file.name)
            
    return errors

def search_processor(file_list, txt_dict, logger):
    """Search OBPG for file list and populate txt dictionary.
    
    Return list of files where error occured.
    """
    
    errors = []
    for file in file_list:
        if file.name.startswith("refined_"):
            timestamp = file.name.split("_")[1][1:].split('.')[0].replace('T', '')
            file_name = PROC_DICT[file.name.split("_")[1][0]]
        else:
            timestamp = file.name.split('.')[0][1:].replace('T', '')
            file_name = PROC_DICT[file.name.split('.')[0][0]]
        response = query_obpg(timestamp, file_name)
        if response == "No Results Found": 
            errors.append(file.name)
            logger.error(f"Could not locate: {file.name} in CMR.")
        else:
            
            # Sort quicklook and refined, ignoring any other files returned
            nrt = []
            ref = []
            response = response.split('\n')
            for element in response:
                if "IOP" in element or "LAND" in element or "PICT" in element: continue
                if "NRT" in element: nrt.append(element)
                else: ref.append(element)
            
            dataset = DATASET_DICT[file_name]
            # Return refined if available
            if len(ref) > 0:
                logger.info(f"Located refined files for: {file.name} in CMR.")
                for url in ref:
                    txt_dict[dataset].append([f"{GET_FILE_URL}/{url.split(' ')[2]}", url.split(' ')[0]])
            elif len(nrt) > 0:
                logger.info(f"Located quicklook files for: {file.name} in CMR.")
                for url in nrt:
                    txt_dict[dataset].append([f"{GET_FILE_URL}/{url.split(' ')[2]}", url.split(' ')[0]])
            else:
                errors.append(file.name)
                logger.error(f"Could not locate: {file.name} in CMR.")
            
    return errors
        
def query_obpg(timestamp, file_name, file_type=None):
    """Query OBPG for file and checksum."""
    
    date = datetime.datetime.strptime(timestamp, "%Y%m%d%H%M%S")
    sdate = date.strftime("%Y-%m-%d %H:%M:%S")
    edate = (date + datetime.timedelta(0,5)).strftime("%Y-%m-%d %H:%M:%S")
    if file_type:
        search = f"{file_name}*{date.year}*{file_type}*.nc"
    else:
        search = f"{file_name}*{date.year}*.nc"
    params = {
        "sensor": DATASET_DICT[file_name],
        "dtype": "L2",
        "addurl": 1,
        "cksum": 1,
        "results_as_file": 1,
        "sdate": sdate,
        "edate": edate,
        "search": search,
        "std_only": 0
    }
    req = requests.get(url=SEARCH_URL, params=params)
    return req.text.strip()

def create_txt_files(txt_dict, logger):
    """Create downloader text file."""
    
    txt_dir = pathlib.Path("/tmp/generate")
    txt_dir.mkdir(parents=True, exist_ok=True)
    date = datetime.datetime.now(datetime.timezone.utc)
    txt_files = { "aqua": [], "terra": [], "viirs": []}
    for dataset, downloads in txt_dict.items():
        
        unique_downloads = remove_duplicates(downloads)
        
        # Quicklook text creation        
        quicklook = [ download for download in unique_downloads if "NRT" in download[0] ]
        if len(quicklook) > 0:
            quicklook_txt_file = f"{dataset}_quicklook_{date.year}_{date.month}_{date.day}_{date.hour}_{date.minute}_{date.second}_{random.randint(1000,9999)}.txt"
            write_txt(quicklook, txt_dir.joinpath(quicklook_txt_file))
            txt_files[dataset].append(txt_dir.joinpath(quicklook_txt_file))
            logger.info(f"Create quicklook TXT file: {txt_dir.joinpath(quicklook_txt_file)}")
        
        # Refined text creation
        refined = [ download for download in unique_downloads if not "NRT" in download[0] ]
        if len(refined) > 0:
            refined_txt_file = f"{dataset}_refined_{date.year}_{date.month}_{date.day}_{date.hour}_{date.minute}_{date.second}_{random.randint(1000,9999)}.txt"
            write_txt(refined, txt_dir.joinpath(refined_txt_file))
            txt_files[dataset].append(txt_dir.joinpath(refined_txt_file))
            logger.info(f"Create refined TXT file: {txt_dir.joinpath(refined_txt_file)}")
            
    return txt_files
        
def remove_duplicates(downloads):
    """Remove duplicate downloads from list"""
    
    unique = []
    for download in downloads:
        if download in unique:
            continue
        else:
            unique.append(download)
    return unique
        
def write_txt(downloads, txt_filename):
    """Write downloads to text file."""
    
    downloads.sort(reverse=True)    # Sort descending
    with open(txt_filename, 'w') as fh:
        for download in downloads:
            fh.write(f"{download[0]} {download[1]}\n")

def upload_txt(prefix, txt_dict, logger):
    """Upload text lists to S3 bucket."""

    s3 = boto3.client("s3")
    try:
        for dataset, txts in txt_dict.items():
            for txt in txts:
                s3.upload_file(str(txt), f"{prefix}", f"download-lists/{dataset}/{txt.name}", ExtraArgs={"ServerSideEncryption": "aws:kms"})
                logger.info(f"Uploaded: s3://{prefix}/download-lists/{dataset}/{txt.name}.")
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error encountered uploading text files to: s3://{prefix}/download-lists/{dataset}.")
        raise e
    
def archive_files(combiner_file_list, combiner_error_logs, processor_file_list, 
                  processor_error_logs, logger):
    """Compress quarantined files and errors logs then remove files from EFS.
    
    Also removes downloads .hidden file to prevent downloader errors.
    """
    
    # Archive location and creation
    archive_dir = DATA_DIR.joinpath("archive", "error_checker")
    archive_dir.mkdir(parents=True, exist_ok=True)
    datetime_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S")
    
    zipped = []
    if len(combiner_file_list) > 0 or len(combiner_error_logs) > 0:
        combiner_zip = archive_dir.joinpath(f"combiner_{datetime_str}.zip")
        with zipfile.ZipFile(combiner_zip, mode='w') as archive:
            for file in combiner_file_list: 
                archive.write(file, arcname=file.name)
                logger.info(f"{file} written to {combiner_zip}.")
            for file in combiner_error_logs: 
                archive.write(file, arcname=file.name)
                logger.info(f"{file} written to {combiner_zip}.")
            logger.info(f"Archive created: {combiner_zip}.")
        zipped.append(combiner_zip)
    
    if len(processor_file_list) > 0 or len(processor_error_logs) > 0:    
        processor_zip = archive_dir.joinpath(f"processor_{datetime_str}.zip")
        with zipfile.ZipFile(processor_zip, mode='w') as archive:
            for file in processor_file_list: 
                archive.write(file, arcname=file.name)
                logger.info(f"{file} written to {processor_zip}.")
            for file in processor_error_logs: 
                archive.write(file, arcname=file.name)
                logger.info(f"{file} written to {processor_zip}.")
            logger.info(f"Archive created: {processor_zip}.")
        zipped.append(processor_zip)
    
    # Remove files from EFS    
    delete_list = combiner_file_list + combiner_error_logs + processor_file_list + processor_error_logs
    for file in delete_list: 
        file.unlink()
        logger.info(f"Deleted: {file}")
    
    # Remove downloads .hidden directory
    remove_hidden_downloads(combiner_file_list, processor_file_list, logger)
    
    return zipped

def remove_hidden_downloads(combiner_file_list, processor_file_list, logger):
    """Remove Attempt to remove any downloads that may be tracked in the 
    combiner downloads .hidden directory"""
    
    download_dir = DATA_DIR.joinpath("combiner", "downloads")
    for combiner_file in combiner_file_list:
        if "LAC_GSST" in combiner_file.name or "SNPP_GSST" in combiner_file.name: continue   # Skip failed combined file
        hidden_dir = download_dir.joinpath(DOWNLOAD_DICT[combiner_file.name.split('.')[0]], ".hidden", combiner_file.name)
        if hidden_dir.is_dir():
            if hidden_dir.exists(): os.rmdir(hidden_dir)
        else:
            if hidden_dir.exists(): hidden_dir.unlink()
        logger.info(f"Removed from EFS: {hidden_dir}.")
    
    for processor_file in processor_file_list:
        if processor_file.name.startswith("refined_"):
            timestamp = processor_file.name.split("_")[1][1:].split('.')[0]
            file_name = PROC_DICT[processor_file.name.split("_")[1][0]]
        else:
            timestamp = processor_file.name.split('.')[0][1:]
            file_name = PROC_DICT[processor_file.name.split('.')[0][0]]
        
        search_dir = download_dir.joinpath(DOWNLOAD_DICT[file_name], ".hidden")
        search_file = f"{file_name}.{timestamp}.L2.*"
        
        dir_list = glob.glob(f"{search_dir}/{search_file}")
        for hidden_dir in dir_list: 
            if os.path.isdir(hidden_dir):
                os.rmdir(hidden_dir)
            else:
                os.remove(hidden_dir)
            logger.info(f"Removed from EFS: {hidden_dir}.")

def upload_archive(prefix, zipped, logger):
    """Uploaded archived quarantine files to S3 bucket and then delete zip from 
    file system."""
    
    s3 = boto3.client("s3")
    year = datetime.datetime.now().year
    try:
        for zip in zipped:
            # Upload file to S3
            s3.upload_file(str(zip), prefix, f"archive/error_checker/{year}/{zip.name}", ExtraArgs={"ServerSideEncryption": "aws:kms"})
            logger.info(f"Uploaded: s3://{prefix}/archive/error_checker/{year}/{zip.name}.")
            
            # Delete file from EFS
            zip.unlink()
            logger.info(f"File deleted: {zip}.")
            
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error encountered uploading zip files to: s3://{prefix}/archive/{year}/.")
        raise e
    
def publish_to_pending(txt_dict, prefix, account, region, logger):
    """Publish txt lists to pending jobs SQS Queue."""
    
    sqs = boto3.client("sqs")
    
    for dataset, txts in txt_dict.items():
        t_list = [txt.name for txt in txts]
        if len(t_list) > 0:
            try:
                response = sqs.send_message(
                    QueueUrl=f"https://sqs.{region}.amazonaws.com/{account}/{prefix}-pending-jobs-{dataset}.fifo",
                    MessageBody=json.dumps(t_list),
                    MessageDeduplicationId=f"{prefix}-{dataset}-{random.randint(1000,9999)}",
                    MessageGroupId = f"{prefix}-{dataset}"
                )
                logger.info(f"Updated queue: https://sqs.{region}.amazonaws.com/{account}/{prefix}-pending-jobs.")
                logger.info(f"Published {dataset} list: {t_list}.")
            except botocore.exceptions.ClientError as e:
                logger.error(f"Error publishing to https://sqs.{region}.amazonaws.com/{account}/{prefix}-pending-jobs-{dataset}.fifo queue.")
                raise e

def report_errors(prefix, combiner_error_list, processor_error_list, s3_error, sqs_error, logger):
    """Report on any errors that occured during execution."""
    
    message = f"The Error Checker component has encountered an error.\n\n"
    
    # Error files
    message += f"The following files were quarantined but could not be located in OBPG to restart the Generate workflow: \n"
    if len(combiner_error_list) > 0:
        message += f"\tCOMBINER FILES: \n"
        for error_file in combiner_error_list:
            message += f"\t\t{error_file}\n"
    
    if len(processor_error_list) > 0:    
        message += f"\tPROCESSOR FILES: \n"
        for error_file in processor_error_list:
            message += f"\t\t{error_file}\n"
    
    if len(combiner_error_list) > 0 or len(processor_error_list) > 0:
        date = datetime.datetime.now(datetime.timezone.utc)
        message += f"\nYou can find the files here: 's3://{prefix}/archive/error_checker/{date.year}' under the following date: {date.year}-{date.month}-{date.day}-{date.hour}:XX:XX...\n"
        
    # S3 error
    if s3_error:
        message += f"\nEncounted error uploading to S3 bucket.\n"
        message += f"{s3_error}\n"
    
    # SQS error
    if sqs_error:
        message += f"\nEncounted error publishing to pending jobs queue.\n"
        message += f"{sqs_error}\n"
        
    publish_event(message, logger)
    
def publish_event(message, logger):
    """Publish event to SNS Topic."""
    
    sns = boto3.client("sns")
    
    # Get topic ARN
    try:
        topics = sns.list_topics()
    except botocore.exceptions.ClientError as e:
        logger.error("Failed to list SNS Topics.")
        logger.error(f"Error - {e}")
        sys.exit(1)
    for topic in topics["Topics"]:
        if TOPIC_STRING in topic["TopicArn"]:
            topic_arn = topic["TopicArn"]
            
    # Publish to topic
    subject = f"Generate Error Checker Lambda Failure"
    try:
        response = sns.publish(
            TopicArn = topic_arn,
            Message = message,
            Subject = subject
        )
        logger.info(f"Error published to: {topic_arn}.")
    except botocore.exceptions.ClientError as e:
        logger.error(f"Failed to publish to SNS Topic: {topic_arn}.")
        logger.error(f"Error - {e}")
        sys.exit(1)
    
    logger.info(f"Message published to SNS Topic: {topic_arn}.")

def remove_tmp(txt_list):
    """Remove temporary txt files."""
    
    for txt_files in txt_list.values():
        for txt_file in txt_files: txt_file.unlink()
