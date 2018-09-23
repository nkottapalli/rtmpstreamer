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
	ingestor = streamer.Ingestor('rtmp://sourcedomain.com/appname/', 'streamkey', 1280, 720)
	pipein = ingestor.initialize()
	broadcastor = streamer.Broadcastor('rtmp://destinationdomain.com/appname/','streamkey', 1280, 720, 'rtmp://sourcedomain.com/appname/streamkey')
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
