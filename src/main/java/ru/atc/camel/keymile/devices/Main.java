package ru.atc.camel.keymile.devices;

//import java.io.File;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.camel.model.ModelCamelContext;
import org.apache.camel.model.dataformat.JsonDataFormat;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.at_consulting.itsm.event.Event;

import javax.jms.ConnectionFactory;

//import org.apache.camel.ComponentConfiguration;
//import org.apache.camel.Header;
//import org.apache.camel.LoggingLevel;
//import org.apache.camel.Message;
//import org.apache.camel.component.cache.CacheComponent;
//import org.apache.camel.component.cache.CacheConfiguration;
//import org.apache.camel.component.cache.CacheConstants;
//import org.apache.camel.component.cache.CacheManagerFactory;
//import net.sf.ehcache.CacheManager;
//import net.sf.ehcache.config.CacheConfiguration;
//import net.sf.ehcache.config.Configuration;
//import net.sf.ehcache.config.PersistenceConfiguration;
//import net.sf.ehcache.config.PersistenceConfiguration.Strategy;
//import net.sf.ehcache.management.CacheManager;
//import org.apache.camel.processor.cache.CacheBasedMessageBodyReplacer;
//import org.apache.camel.processor.cache.CacheBasedTokenReplacer;
//import org.apache.camel.processor.idempotent.FileIdempotentRepository;
//import org.apache.log4j.Level;
//import ru.atc.camel.opsm.events.OPSMConsumer;



public class Main {
	
	public static ModelCamelContext context;
	public static String activemq_port = null;
	public static String activemq_ip = null;
	public static String postgresql_ip = null;
	public static String postgresql_port = null;
	private static Logger logger = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) throws Exception {
		
		logger.info("Starting Custom Apache Camel component example");
		logger.info("Press CTRL+C to terminate the JVM");
		
		if ( args.length == 2  ) {
			activemq_port = (String)args[1];
			activemq_ip = (String)args[0];
		}

		if (activemq_port == null || activemq_port.equals(""))
			activemq_port = "61616";
		if (activemq_ip == null || activemq_ip.equals(""))
			activemq_ip = "172.20.19.195";
		
		logger.info("activemq_ip: " + activemq_ip);
		logger.info("activemq_port: " + activemq_port);

		
		org.apache.camel.main.Main main = new org.apache.camel.main.Main();
		main.enableHangupSupport();
		
		main.addRouteBuilder(new RouteBuilder() {
			
			@Override
			public void configure() throws Exception {
				
				JsonDataFormat myJson = new JsonDataFormat();
				myJson.setPrettyPrint(true);
				myJson.setLibrary(JsonLibrary.Jackson);
				myJson.setJsonView(Event.class);
				
				context = getContext();
				
				PropertiesComponent properties = new PropertiesComponent();
				properties.setLocation("classpath:keymile.properties");
				properties.setEncoding("UTF-8");
				context.addComponent("properties", properties);

				ConnectionFactory connectionFactory = new ActiveMQConnectionFactory
						("tcp://" + activemq_ip + ":" + activemq_port);		
				context.addComponent("activemq", JmsComponent.jmsComponentAutoAcknowledge(connectionFactory));
				
				//context.addComponent("cache", CacheComponent());
				
				//CacheManagerFactory cacheManagerFactory = new CacheManagerFactory();
				
				logger.info("*****context: " + 
						context);
				/*
				CacheConfiguration config=new CacheConfiguration("ServerCacheTest",1500)
						.timeToIdleSeconds(172800)
						.timeToLiveSeconds(172800)
						.diskExpiryThreadIntervalSeconds(172800)
						.eternal(true)
						.overflowToOffHeap(true);
				*/
				
				/*
				net.sf.ehcache.CacheManager cacheManager = new net.sf.ehcache.CacheManager(configurationFileName);
				context.addComponent("activemq", JmsComponent.jmsComponentAutoAcknowledge(connectionFactory));
				*/
				
				KeymileConsumer.setContext(context);
				
		       	//LoggingLevel error = null;
				from("keymile://devices?"
		    			+ "delay={{delay}}&"
		    			+ "username={{username}}&"
		    			+ "password={{password}}&"
		    			+ "postgresql_host={{postgresql_host}}&"
		    			+ "postgresql_db={{postgresql_db}}&"
		    			+ "postgresql_port={{postgresql_port}}&"
		    			+ "serviceNodeGroup={{serviceNodeGroup}}&"
		    			+ "source={{source}}&"
		    			+ "adaptername={{adaptername}}")
				.marshal(myJson)
	    		.choice()
				.when(header("Type").isEqualTo("Error"))
					//.marshal(myJson)
					.to("activemq:{{eventsqueue}}")
					.log("*** Error: ${id} ")
				.otherwise()
					.to("activemq:{{devicesqueue}}")
					.log("*** Device: ${id} ${header.DeviceId} ${header.DeviceType}");
				
				
				// Heartbeats
				from("timer://foo?period={{heartbeatsdelay}}")
		        .process(new Processor() {
					public void process(Exchange exchange) throws Exception {
						KeymileConsumer.genHeartbeatMessage(exchange);
					}
				})
				//.bean(WsdlNNMConsumer.class, "genHeartbeatMessage", exchange)
		        .marshal(myJson)
		        .to("activemq:{{heartbeatsqueue}}")
				.log("*** Heartbeat: ${id}");
			}
		});
		
		main.run();
		
	}

}