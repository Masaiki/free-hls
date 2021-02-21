import os
# import re
import shutil
import time
import math
import argparse
# from sys import argv
from os import getenv as _
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import api, execstr, tsfiles, safename, sameparams
import importlib
import base64
import hashlib
from jinja2 import FileSystemLoader, Environment
load_dotenv()
env = Environment(loader=FileSystemLoader('./web/templates'))
template = env.get_template('play.html')
upload_drive = None


def md5(s):
    md5 = hashlib.md5(s.encode('utf-8')).hexdigest()
    return md5[8:24]


def writefile(code, title=None):
    key = md5(code)
    meta = {
        'title': title or 'untitled',
        'code': base64.b64encode(code.encode('utf-8')).decode('ascii')
    }
    with open(f"play/{key}.html", "w", encoding='utf-8') as f:
        f.write(template.render(meta=meta))


def publish(code, title=None):
    if _('NOSERVER') == 'YES':
        print('The m3u8 file has been dumped to tmp/out.m3u8')
        return
    r = api('POST', 'publish', {'code': code, 'title': title})
    if r:
        url = '%s/play/%s' % (_('APIURL'), r)
        print('This video has been published to: %s' % url)
        print('You can also download it directly: %s.m3u8' % url)
        return url


def video_duration(file):
    return float(execstr(['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', file]))


def genrepair(file, newfile, maxbits):
    maxrate = maxbits / math.ceil(video_duration(file))
    subcmd = 'ffmpeg -y -i %s -copyts -vsync 0 -muxdelay 0 -c:v %s -c:a copy -bsf:v h264_mp4toannexb -b:v %s -pass %s' % (file, 'libx264', maxrate*0.85, newfile)
    return '%s && %s' % (subcmd.replace('-pass', '-pass 1'), subcmd.replace('-pass', '-pass 2'))


def bit_rate(file):
    return int(execstr(['ffprobe', '-v', 'error', '-show_entries', 'format=bit_rate', '-of', 'default=noprint_wrappers=1:nokey=1', file]))


def video_codec(file):
    codecs = execstr(['ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=codec_name', '-of', 'default=noprint_wrappers=1:nokey=1', file])
    return 'h264' if set(codecs.split('\n')).difference({'h264'}) else 'copy'


def get_segment_time(file, segment_specify):
    if segment_specify.isnumeric():
        return float(segment_specify)
    rate = bit_rate(file)
    segment_time = int((20 << 23) / rate / 4)
    return segment_time


def command_generator(file, segment_specify):
    sub = ' -segment_time %d' % segment_specify
    vcodec = video_codec(file)
    # LIMITED
    # if rate > 6e6 or segment_specify == 'LIMITED':
    #     br = min(rate, 15e6)
    #     sub += ' -b:v %d -maxrate %d -bufsize %d' % (br, 16e6, 16e6/1.5)
    #     vcodec, segment_time = 'h264', 5
    # SEGMENT_TIME
    # print('auto', segment_time, rate)
    return 'ffmpeg -i %s -vcodec %s -acodec aac -bsf:v h264_mp4toannexb -map 0:v:0 -map 0:a? -f segment -segment_list out.m3u8 %s out%%05d.ts' % (safename(file), vcodec, sub)


def uploader():
    handle = upload_drive.handle

    def wrapper(file):
        with open(file, 'rb') as f:
            if os.path.getsize(file) < upload_drive.UPLOAD_LIMIT:
                return handle(f)
    return wrapper


def main(video_path, video_title, segment_specify, repair=False):
    global upload_drive
    cwd = os.getcwd()
    upload_drive = importlib.import_module('uploader.' + _('UPLOAD_DRIVE'))
    title = video_title if video_title else os.path.splitext(os.path.basename(video_path))[0]
    tmpdir = os.path.dirname(os.path.abspath(__file__)) + '/tmp'
    segment_time = get_segment_time(os.path.abspath(video_path), segment_specify)
    command = command_generator(os.path.abspath(video_path), segment_time)
    if not os.path.exists(tmpdir):
        os.mkdir(tmpdir)
    if sameparams(tmpdir, command):
        os.chdir(tmpdir)
    else:
        os.chdir(tmpdir)
        try:
            os.system(command)
        except KeyboardInterrupt:
            return 1
        with open('command.sh', 'w') as f:
            f.write(command)
    failures, completions = 0, 0
    with open('out.m3u8', 'r') as f:
        lines = f.read()
    for tsfile in tsfiles(lines):
        if os.path.getsize(tsfile) >= upload_drive.UPLOAD_LIMIT:
            if repair:
                tmp = 'rep.%s' % tsfile
                os.system(genrepair(tsfile, tmp, upload_drive.UPLOAD_LIMIT * 8))
                os.rename(tsfile, 'old.%s' % tsfile)
                os.rename(tmp, tsfile)
            else:
                os.chdir(cwd)
                return 1
    executor = ThreadPoolExecutor(max_workers=10)
    futures = {executor.submit(uploader(), chunk): chunk for chunk in tsfiles(lines)}
    for future in as_completed(futures):
        completions += 1
        result = future.result()
        if not result:
            failures += 1
            print('[%s/%s] Uploaded failed: %s' % (completions, len(futures), futures[future]))
            continue
        lines = lines.replace(futures[future], result)
        print('[%s/%s] Uploaded %s to %s' % (completions, len(futures), futures[future], result))
    print('\n')
    # Write to file
    with open('out.m3u8', 'w') as f:
        f.write(lines)
    os.chdir(cwd)
    if not failures:
        shutil.copy2('./tmp/out.m3u8', f'./results/{int(time.time())}.m3u8')
        writefile(lines, title)
        shutil.rmtree(tmpdir)
        return 0
    else:
        print('Partially successful: %d/%d' % (completions, completions-failures))
        print('You can re-execute this program with the same parameters')
        return 2


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('file', help='File to be sliced')
    parser.add_argument('--segment', '-s', default='', help='Specify the segement time')
    parser.add_argument('--title', '-t', default='', help='Title for generating html')
    args = parser.parse_args()
    main(args.file, args.title, args.segment)
