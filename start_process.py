import logging
import cv2

import rtmpstreamer as streamer

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(process)d:%(levelname)s:%(message)s')
file_handler = logging.FileHandler('logs/process.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


#logging.basicConfig(filename='process.log', level=logging.DEBUG, format='%(asctime)s:%(levelname)s:%(module)s:%(message)s')

def main():
	ingestor = streamer.Ingestor('rtmp://www.thezerogames.com/livere/', 'haZhXN9YntVSEfSu', 1280, 720, '/home/nkottapalli/livespott/rtmpingestor/test/vpipe/')
	pipein = ingestor.initialize()
	broadcastor = streamer.Broadcastor('rtmp://10.0.0.248/live/','haZhXN9YntVSEfSu', 1280, 720, 'rtmp://www.thezerogames.com/livere/haZhXN9YntVSEfSu')
	pipeout = broadcastor.initialize()
	while(ingestor.more()):
		logger.debug('Frame Processing Start')
		output = ingestor.read()
		broadcastor.write(output)
		ingestor.update()
		ingestor.kinterruptwatch()
		broadcastor.kinterruptwatch()
		logger.debug('Frame Processing Done')
		if cv2.waitKey(25) & 0xFF == ord('q'):
			break

main()
