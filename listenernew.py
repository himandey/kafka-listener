import urllib2
import json
import time
from datetime import datetime, timedelta
from yahoo_finance import Share

from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

#set share tickers here
tickers = ['YHOO', 'SBUX', 'AAPL','AIG','AMZN','AXP','BAC','BIS','EBAY','GE','HD','HP','IBM','MSFT','NKE','ORCL','PEP','PG','T','TWX','TXN','VZ','WFC','XOM','UPS','WMT','CSCO','COF','CL','CCE','CBS','CAT','C','BRK.B','BBY','BBBY','BA','AON','ADSK']

date = datetime.now()
twodaysago = date.today() - timedelta(4)

def publishweather(data):
	out = data.encode('utf-8')
	producer.send_messages('weather', out)
	#print data
	return True	   
	
if __name__ == '__main__':
	Connect to Kafka 
	client = KafkaClient("ip-172-31-35-118.ec2.internal:6667")
	producer = SimpleProducer(client)
	print 'Listening...'
	
	print 'Publishing stock data to kafka topic'
	
	
	print 'Publishing weather data to kafka topic'
	while True:
		f = urllib2.urlopen('http://api.wunderground.com/api/4bca86c5206d3b3f/forecast10day/q/NY/New_York.json')
		json_string = f.read()
		parsed_json = json.loads(json_string)
		dataout = {}
		

		#get next hour's forecast, element 0 in array
		forecast = parsed_json['forecast']['simpleforecast']['forecastday'][1]
		maxtemp_f = forecast['high']['fahrenheit']
		mintemp_f = forecast['low']['fahrenheit']
		meanhumid = forecast['avehumidity']
		maxhumid = forecast['maxhumidity']
		minhumid = forecast['minhumidity']
		precip = forecast['qpf_allday']['in']
		winddir = forecast['avewind']['degrees']
				
		#build json object
		dataout['maxtemp_f'] = maxtemp_f
		dataout['mintemp_f'] = mintemp_f
		dataout['meanhumid'] = meanhumid
		dataout['maxhumid'] = maxhumid
		dataout['minhumid'] = minhumid
		dataout['precip'] = precip
		dataout['winddir'] = winddir
		
		for ticks in tickers:
			tick = Share(ticks)
			yestclose = tick.get_prev_close().encode('ascii', 'ignore')
			twod = str(twodaysago.strftime('%Y-%m-%d'))
			tod = str(date.today().strftime('%Y-%m-%d'))
			twoyestclose = tick.get_historical(twod, tod)
			tyclose = twoyestclose[1]['Close'].encode('ascii', 'ignore')
			diff = float(yestclose) - float(tyclose)
			dataout['ticker'] = ticks
			dataout['close'] = float(yestclose)
			dataout['tyclose'] = float(tyclose)
			dataout['diff'] = diff
			#no quantitative visiblity data or wet bulb temperature
			json_out = json.dumps(dataout)
			publishweather(json_out)
		f.close()
		time.sleep(900) #wait 15 minutes
