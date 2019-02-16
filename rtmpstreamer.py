import subprocess as sp
import numpy as np

import datetime
import time
import os
import cv2
import copy
import logging

from logging.handlers import TimedRotatingFileHandler as trfh

ingestor_logger = logging.getLogger(__name__)
ingestor_logger.setLevel(logging.INFO)
ingestor_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(funcName)s:%(message)s')
ingestor_handler = trfh('logs/ingestor.log', when='m', interval=1, backupCount=5)
ingestor_handler.setFormatter(ingestor_formatter)
ingestor_logger.addHandler(ingestor_handler)

#broadcastor_logger = logging.getLogger(__name__)
#broadcastor_logger.setLevel(logging.INFO)
#broadcastor_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(funcName)s:%(message)s')
#broadcastor_file_handler = logging.FileHandler('logs/broadcastor.log')
#broadcastor_file_handler.setFormatter(broadcastor_formatter)
#broadcastor_logger.addHandler(broadcastor_file_handler)

# Ingestor : rtmp://domain.appname.streamkey -> videopipe compatible with numpy <=> opencv
class Ingestor:
	def __init__(self, source, streamkey, width, height):
		self.source = source
		self.streamkey = streamkey
		self.width = width
		self.height = height
		self.bin = 'ffmpeg'
		self._address = self.source + self.streamkey
		self._cmdx = [self.bin,'-loglevel','quiet','-i',self._address,'-f','rawvideo','-tune','zerolatency','-fflags','nobuffer','-preset','ultrafast','-pix_fmt','bgr24','-c:v','rawvideo','-']
		self._vpipe = sp.Popen(self._cmdx, stdout=sp.PIPE, bufsize=10**8)
		self.stopped = False
		self._grabbed = False
		self.start_time = 0
		self.total_frames = 0
		self.fps = 0
		self.frames_reads = 0
		self.last_frame_readat = 0
		self.rfps = 0
		self.first_readat = 0

	def initialize(self):
		self.start_time = time.time()
		ingestor_logger.info('STARTED_AT= %s'%(str(datetime.datetime.now())))
		self.update()
		return self

	def update(self):
		while(True):
			self.total_frames += 1
			if self.stopped:
				self.prox.terminate()
				return
			self._frame = self._vpipe.stdout.read(self.width*self.height*3)
			if len(self._frame) > 0:
				self._grabbed = True
			else:
				self._grabbed = False
			self._vpipe.stdout.flush()
			total_time = time.time() - self.start_time
			self.fps = self.total_frames / total_time
			ingestor_logger.info('FPS= %s, FRAMES= %s, ELAPSED= %s'%(str(self.fps),str(self.total_frames), str(total_time)))
			return

	def read(self):
		self.frames_reads += 1
		if self.frames_reads <= 1:
			self.first_readat = time.time()
			self.last_frame_readat = self.start_time
		_vframe = np.fromstring(self._frame,dtype='uint8')
		_vframe = _vframe.reshape((self.height, self.width, 3))
		cv2.putText(_vframe,'Copyright YYYY, Comany Name Goes Here.', (5, 15), cv2.FONT_HERSHEY_SIMPLEX, 0.4, (255, 255, 255), 1, cv2.LINE_AA)
		elapsed_last_read = time.time() - self.last_frame_readat
		rps = 1 / elapsed_last_read
		self.rfps = self.frames_reads / (time.time()-self.first_readat)
		self.last_frame_readat = time.time()
		ingestor_logger.info('RPS= %s, FPS= %s, READS= %s, TIME= %s'%(str(rps), str(self.rfps), str(self.frames_reads), str(elapsed_last_read)))
		return _vframe

	def more(self):
		return self._grabbed

	def stop(self):
		self.stopped = True

	def kinterruptwatch(self):
		if cv2.waitKey(25) & 0xFF == ord('q'):
			ingestor_logger.info('STOPPED_AT= %s'%(str(datetime.datetime.now())))
			self.stop()


# Broadcastor : videopipe compatible with numpy <=> opencv -> rtmp://domain.appname.streamkey
#(note : sudio taken from source and delay can be adjusted to sync AV in output stream)
#(issuse : AV synchronization)
class Broadcastor:
	def __init__(self, destination, streamkey, width, height, sourceurl):
		self.destination = destination
		self.streamkey = streamkey
		self.width = width
		self.height = height
		self.bin = 'ffmpeg'
		self.address = self.destination + self.streamkey
		self.videosize = str(self.width) + 'X' + str(self.height)
		self.audiodelay = 2.00
		self.sourceurl = sourceurl
		self.cmdx = [self.bin, '-loglevel', 'info', '-y', '-thread_queue_size', '4096', '-f', 'rawvideo', '-pix_fmt', 'bgr24', '-video_size', self.videosize, '-i', '-', '-itsoffset', str(self.audiodelay), '-i', self.sourceurl, '-map', '0:v:0', '-map', '1:a:0', '-c:v', 'libx264', '-b:v', '2M', '-maxrate', '2M', '-bufsize', '1M', '-pix_fmt', 'yuv420p', '-x264opts', 'keyint=30:min-keyint=2:no-scenecut', '-c:a', 'copy', '-b:a', '128k', '-preset', 'ultrafast', '-f', 'flv','-use_wallclock_as_timestamps','1' ,self.address]
		self._vpipe = sp.Popen(self.cmdx, stdin=sp.PIPE, shell=False)
		self._frame = ''
		self.stopped = False
		self._pushed = False

	def initialize(self):
		sframe = np.zeros((self.height, self.width, 3), np.uint8)
		status = self.write(sframe)
		return self

	def write(self, frame):
		if self.stopped:
			self._vpipe.terminate()
			return
		self._frame = frame
		if len(self._frame) > 0:
			self._vpipe.stdin.write(self._frame.tostring())
			self._pushed = True
		else:
			self._pushed = False
		return self._pushed

	def stop(self):
		self.stopped = True

	def kinterruptwatch(self):
		if cv2.waitKey(25) & 0xFF == ord('q'):
			self.stop()


