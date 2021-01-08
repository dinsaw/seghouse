import logging
import subprocess
import uuid

TMP_DIR_PREFIX = "seghouse"


def s3_copy(s3_path, local_dir_path):
    command = ["aws", "s3", "cp", s3_path, local_dir_path, "--recursive", "--exclude", "*", "--include", "*.gz"]
    logging.info(f"command = {command}")
    process = subprocess.run(command)
    process.check_returncode()


def download_gz_files(s3_dir):
    local_dir_name = str(uuid.uuid4()).replace("-", "")
    local_dir_path = f"/tmp/{TMP_DIR_PREFIX}-{local_dir_name}"
    subprocess.run(["mkdir", "-p", local_dir_path])

    logging.info(f"Copying files to {local_dir_path}")
    s3_copy(s3_dir, local_dir_path)

    return local_dir_path
