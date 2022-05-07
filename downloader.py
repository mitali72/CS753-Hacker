import sys
import os
from multiprocessing.pool import ThreadPool

from yt_dlp import YoutubeDL
import ffmpeg
from moviepy.video.io.ffmpeg_tools import ffmpeg_extract_subclip
import moviepy.editor as mp

class VidInfo:
    def __init__(self, yt_id, start_time, end_time, out_dir1, out_dir2):
        self.yt_id = yt_id
        self.start_time = round(float(start_time))
        self.end_time = round((float(end_time)+4))
        self.end_time += (self.end_time-self.start_time)%2
        print(self.start_time,self.end_time)
        self.out_filename = os.path.join(out_dir1, yt_id + '_' + str(self.start_time) + '_' + str(self.end_time) + '.mp4')
        self.out_audio_filename=os.path.join(out_dir2, yt_id +'_audio.wav')


def download(vidinfo):

    yt_base_url = 'https://www.youtube.com/watch?v='

    yt_url = yt_base_url+vidinfo.yt_id

    ydl_opts = {
        'format': '22/18',
        'quiet': True,
        'ignoreerrors': True,
        'no_warnings': True,
    }
    #print(vidinfo.end_time)
    try:
        with YoutubeDL(ydl_opts) as ydl:
            download_url = ydl.extract_info(url=yt_url, download=False)['url']
    except:
        return_msg = '{}, ERROR (youtube)!'.format(vidinfo.yt_id)
        return return_msg
    try:
      ffmpeg_extract_subclip(download_url, vidinfo.start_time, vidinfo.end_time, targetname=vidinfo.out_filename)
      my_clip = mp.VideoFileClip(vidinfo.out_filename)
      my_clip.audio.write_audiofile(vidinfo.out_audio_filename)
      #os.system("!ffmpeg -hide_banner -i "$vidinfo.out_filename" -vn -c:a copy "$vidinfo.out_audio_filename"-audio."wav"")
    except:
      return_msg = '{}, ERROR (ffmpeg)!'.format(vidinfo.yt_id)
      return return_msg

      # try:
    #     (
    #         ffmpeg
    #             .input(download_url, ss=vidinfo.start_time, to=vidinfo.end_time)
    #             .output(vidinfo.out_filename, format='mp4', r=25, vcodec='libx264',
    #                     crf=18, preset='veryfast', pix_fmt='yuv420p', acodec='aac', audio_bitrate=128000,
    #                     strict='experimental')
    #             .global_args('-y')
    #             .global_args('-loglevel', 'error')
    #             .run()

    #     )
    # except:
    #     print(vidinfo.start_time,vidinfo.end_time)
    #     return_msg = '{}, ERROR (ffmpeg)!'.format(vidinfo.yt_id)
    #     return return_msg

    return '{}, DONE!'.format(vidinfo.yt_id)


if __name__ == '__main__':

    split = sys.argv[1]
    csv_file = '/content/drive/MyDrive/Hacker/avspeech_{}.csv'.format(split)
    out_dir1 = '/content/train/video/'
    out_dir2 = '/content/train/audio/'

    os.makedirs(out_dir1, exist_ok=True)
    os.makedirs(out_dir2, exist_ok=True)

    with open(csv_file, 'r') as f:
        lines = f.readlines()
        lines = [x.split(',') for x in lines]
        vidinfos = [VidInfo(x[0], x[1], x[2], out_dir1, out_dir2) for x in lines]

    bad_files = open('bad_files_{}.txt'.format(split), 'w')
    results = ThreadPool(5).imap_unordered(download, vidinfos)
    cnt = 0
    for r in results:
        cnt += 1
        print(cnt, '/', len(vidinfos), r)
        if 'ERROR' in r:
            bad_files.write(r + '\n')
    bad_files.close()
