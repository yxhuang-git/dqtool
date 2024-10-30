import sys
import argparse
import logging
import subprocess
from subprocess import PIPE

# ロガーインスタンス生成
logger = logging.getLogger(__name__)
[logger.removeHandler(h) for h in logger.handlers]
log_format = '[%(levelname)s][%(filename)s][%(funcName)s:%(lineno)d]\t%(message)s'
stdout_handler = logging.StreamHandler(stream=sys.stdout)
stdout_handler.setFormatter(logging.Formatter(log_format))
logger.addHandler(stdout_handler)
logger.setLevel(logging.INFO)

def dist_copy(src, dest, srcPattern):
    cmd = ['s3-dist-cp']

    if 's3://' in dest and 's3a://' not in dest:
        dest = dest.replace('s3://', 's3a://')
        
    if 's3://' in src and 's3a://' not in src:
        src = src.replace('s3://', 's3a://')
    
    cmd.extend(['--src', src])
    cmd.extend(['--dest', dest])
    
    result = None
    if len(srcPattern) != 0:
        if '"' in srcPattern:
            srcPattern = srcPattern.replace('"', '')

        srcPattern = srcPattern.split(',')
        for pattern in srcPattern:
            pattern = pattern.strip()
            cmd_op = []
            cmd_op.extend(cmd)
            cmd_op.extend(['--srcPattern', pattern])
            result = subprocess.run(cmd_op, encoding='utf-8', stderr=PIPE)
    else:
        result = subprocess.run(cmd, encoding='utf-8', stderr=PIPE)
    
    if result.returncode != 0:
        logger.error('subprocess failed')
        logger.error(result.stderr)
        raise Exception(result.stderr)

if __name__ in "__main__":
    parser = argparse.ArgumentParser(description='Parser for Command Arguments.')
    parser.add_argument('--src', dest='src')
    parser.add_argument('--dest', dest='dest')
    parser.add_argument('--srcPattern', dest='srcPattern', default=[])
    args = parser.parse_args()

    src = args.src
    dest = args.dest
    srcPattern = args.srcPattern
    dist_copy(src, dest, srcPattern)