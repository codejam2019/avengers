package dataGenerator;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class DataGenerator implements Runnable {


	private final KafkaProducer<Integer, String> producer;
	private final String topicName;
	private final String fileLocation;
	private int fromCityNum;
	private int toCityNum;
	private int numSensorPerCity;
	private long timeStamp;

	DataGenerator(KafkaProducer<Integer, String> producer, String topicName, int fromCityNum, int toCityNum, int numSensorPerCity, long timeStamp) {
		this.producer = producer;
		this.topicName = topicName;
		this.fromCityNum = fromCityNum;
		this.toCityNum = toCityNum;
		this.numSensorPerCity = numSensorPerCity;
		this.timeStamp = timeStamp;
	}
	
	private static int getRandomNumberInRange(int min, int max) {

		if (min >= max) {
			throw new IllegalArgumentException("max must be greater than min");
		}

		Random r = new Random();
		return r.nextInt((max - min) + 1) + min;
	}

	@Override
	public void run() {
		int msgKey = 0;
		

		try {
			for (int i = fromCityNum; i <= toCityNum; i++) {
				StringBuffer msg = new StringBuffer();
				
				for (int j = 0; j < this.numSensorPerCity; j++) {
					msg.append("weather,city=city-" + i);
					msg.append(",");
					msg.append("sensor=" + i + "-" + j + " ");
					msg.append("temp=" + getRandomNumberInRange(10, 50));
					msg.append(",");
					msg.append("rain=" + getRandomNumberInRange(0, 50) + " ");
					msg.append(this.timeStamp);
				}

				producer.send(new ProducerRecord<>(this.topicName, msgKey, msg.toString()));
				msgKey++;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}


	public static void main(String[] args) {
		if (args.length < 5) {
			System.out.println("Please provide command line arguments: numOfCities numOfSensorPerCity numOfThreads kafkaIpAddress timeStamp");
			System.exit(-1);
		}

		int numCities = Integer.parseInt(args[0]);
		int numSensorPerCity = Integer.parseInt(args[1]);

		int numberOfThreads = Integer.parseInt(args[2]);
		
		String kafkaIpAddress = args[3];
		
		long timeStamp = Long.parseLong(args[4]);

		Properties properties = new Properties();

		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaIpAddress + ":" + 9092); 
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				LongSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());

		KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties);

		Thread[] dispatchers = new Thread[numberOfThreads];
		for (int i = 0, fromCity = 0; i < dispatchers.length; i++, fromCity += (fromCity + numCities/dispatchers.length)) {
			DataGenerator dg = new DataGenerator(producer, "topic" + i, fromCity, (fromCity - 1 + numCities/dispatchers.length), numSensorPerCity, timeStamp);
			dispatchers[i] = new Thread();
			dispatchers[i].start();
		}

		try {
			for (Thread t : dispatchers)
				t.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
	}

}
