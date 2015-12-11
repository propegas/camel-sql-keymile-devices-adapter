package ru.atc.camel.keymile.devices;

//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.FileReader;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;

//import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
//import org.apache.camel.Producer;
//import org.apache.camel.ProducerTemplate;
//import org.apache.camel.component.cache.CacheConstants;
import org.apache.camel.impl.ScheduledPollConsumer;
import org.apache.camel.model.ModelCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.at_consulting.itsm.device.Device;

//import com.thoughtworks.xstream.io.json.JsonWriter.Format;

//import com.apc.stdws.xsd.isxcentral._2009._10.ISXCDevice;
//import com.apc.stdws.xsd.isxcentral._2009._10.ISXCAlarmSeverity;
//import com.apc.stdws.xsd.isxcentral._2009._10.ISXCAlarm;
//import com.apc.stdws.xsd.isxcentral._2009._10.ISXCDevice;
//import com.google.gson.Gson;
//import com.google.gson.GsonBuilder;
//import com.google.gson.JsonArray;
//import com.google.gson.JsonElement;
//import com.google.gson.JsonObject;
//import com.google.gson.JsonParser;

//import ru.at_consulting.itsm.device.Device;
import ru.at_consulting.itsm.event.Event;
//import ru.atc.camel.keymile.devices.api.OVMMDevices;
//import ru.atc.camel.keymile.devices.api.OVMMEvents;

import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

//import com.mysql.jdbc.Connection;
//import com.mysql.jdbc.Driver;
import org.postgresql.Driver;



public class KeymileConsumer extends ScheduledPollConsumer {
	
	private String[] openids = {  };
	
	private static Logger logger = LoggerFactory.getLogger(Main.class);
	
	public static KeymileEndpoint endpoint;
	
	public static ModelCamelContext context;
	
	public enum PersistentEventSeverity {
	    OK, INFO, WARNING, MINOR, MAJOR, CRITICAL;
		
	    public String value() {
	        return name();
	    }

	    public static PersistentEventSeverity fromValue(String v) {
	        return valueOf(v);
	    }
	}

	public KeymileConsumer(KeymileEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
        //this.afterPoll();
        this.setTimeUnit(TimeUnit.MINUTES);
        this.setInitialDelay(0);
        this.setDelay(endpoint.getConfiguration().getDelay());
	}
	
	public static ModelCamelContext getContext() {
		// TODO Auto-generated method stub
				return context;
	}
	
	public static void setContext(ModelCamelContext context1){
		context = context1;

	}

	@Override
	protected int poll() throws Exception {
		
		String operationPath = endpoint.getOperationPath();
		
		if (operationPath.equals("devices")) return processSearchDevices();
		
		// only one operation implemented for now !
		throw new IllegalArgumentException("Incorrect operation: " + operationPath);
	}
	
	@Override
	public long beforePoll(long timeout) throws Exception {
		
		logger.info("*** Before Poll!!!");
		// only one operation implemented for now !
		//throw new IllegalArgumentException("Incorrect operation: ");
		
		//send HEARTBEAT
		//genHeartbeatMessage();
		
		return timeout;
	}
	
	private void genErrorMessage(String message) {
		// TODO Auto-generated method stub
		long timestamp = System.currentTimeMillis();
		timestamp = timestamp / 1000;
		String textError = "Возникла ошибка при работе адаптера: ";
		Event genevent = new Event();
		genevent.setMessage(textError + message);
		genevent.setEventCategory("ADAPTER");
		genevent.setSeverity(PersistentEventSeverity.CRITICAL.name());
		genevent.setTimestamp(timestamp);
		
		genevent.setEventsource(String.format("%s", endpoint.getConfiguration().getAdaptername()));
		
		genevent.setStatus("OPEN");
		genevent.setHost("adapter");
		
		logger.info(" **** Create Exchange for Error Message container");
        Exchange exchange = getEndpoint().createExchange();
        exchange.getIn().setBody(genevent, Event.class);
        
        exchange.getIn().setHeader("EventIdAndStatus", "Error_" +timestamp);
        exchange.getIn().setHeader("Timestamp", timestamp);
        exchange.getIn().setHeader("queueName", "Events");
        exchange.getIn().setHeader("Type", "Error");

        try {
			getProcessor().process(exchange);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}

	
	public static void genHeartbeatMessage(Exchange exchange) {
		// TODO Auto-generated method stub
		long timestamp = System.currentTimeMillis();
		timestamp = timestamp / 1000;
		//String textError = "Возникла ошибка при работе адаптера: ";
		Event genevent = new Event();
		genevent.setMessage("Сигнал HEARTBEAT от адаптера");
		genevent.setEventCategory("ADAPTER");
		genevent.setObject("HEARTBEAT");
		genevent.setSeverity(PersistentEventSeverity.OK.name());
		genevent.setTimestamp(timestamp);
		
		genevent.setEventsource(String.format("%s", endpoint.getConfiguration().getAdaptername()));
		
		logger.info(" **** Create Exchange for Heartbeat Message container");
        //Exchange exchange = getEndpoint().createExchange();
        exchange.getIn().setBody(genevent, Event.class);
        
        exchange.getIn().setHeader("Timestamp", timestamp);
        exchange.getIn().setHeader("queueName", "Heartbeats");
        exchange.getIn().setHeader("Type", "Heartbeats");
        exchange.getIn().setHeader("Source", String.format("%s", endpoint.getConfiguration().getAdaptername()));

        try {
        	//Processor processor = getProcessor();
        	//.process(exchange);
        	//processor.process(exchange);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		} 
	}
	
	
	// "throws Exception" 
	private int processSearchDevices()  throws Exception, Error, SQLException {
		
		//Long timestamp;
		BasicDataSource dataSource = setupDataSource();
		
		List<HashMap<String, Object>> listAllDevices = new ArrayList<HashMap<String,Object>>();
		List<HashMap<String, Object>> listClosedEvents = new ArrayList<HashMap<String,Object>>();
		//List<HashMap<String, Object>> listVmStatuses = null;
		int events = 0;
		//int statuses = 0;
		try {
			
			// get All Nodes (units)
			logger.info( String.format("***Try to get All Nodes***"));
			listAllDevices = getAllDevices(dataSource);
			logger.info( String.format("***Received %d Devices from SQL***", listAllDevices.size()));
			
			/*
			List<HashMap<String, Object>> listAllEvents = new ArrayList<HashMap<String,Object>>();
			listAllEvents.addAll(listAllDevices);
			listAllEvents.addAll(listClosedEvents);
			*/
			
			// List<HashMap<String, Object>> allevents = (List<HashMap<String, Object>>) ArrayUtils.addAll(listOpenEvents);
			
			String type, id;
			
			Device gendevice = new Device();
			
			//logger.info( String.format("***Try to get VMs statuses***"));
			for(int i=0; i < listAllDevices.size(); i++) {
			  	
				type = listAllDevices.get(i).get("type").toString();
				id  = listAllDevices.get(i).get("nodeid").toString();
				logger.debug("DB row " + i + ": " + type + 
						" " + id);
			
				gendevice = genDeviceObj(listAllDevices.get(i));
				
				logger.debug("*** Create Exchange ***" );

				String key = gendevice.getId() + "_" +
						gendevice.getName();
				
				Exchange exchange = getEndpoint().createExchange();
				exchange.getIn().setBody(gendevice, Device.class);
				exchange.getIn().setHeader("DeviceId", key);
				exchange.getIn().setHeader("DeviceType", gendevice.getDeviceType());
				
				
				
				//exchange.getIn().setHeader("DeviceType", vmevents.get(i).getDeviceType());

				try {
					getProcessor().process(exchange);
					events++;
					
					//File cachefile = new File("sendedEvents.dat");
					//removeLineFromFile("sendedEvents.dat", "Tgc1-1Cp1_ping_OK");
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					logger.error( String.format("Error while process Exchange message: %s ", e));
				} 
						
					
			}
			
			logger.debug( String.format("***Received %d Keymile Devices from SQL*** ", listAllDevices.size()));
			
           // logger.info(" **** Received " + events.length + " Opened Events ****");
    		
    		
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get Devices from SQL: %s ", e));
			genErrorMessage(e.getMessage() + " " + e.toString());
			dataSource.close();
			return 0;
		}
		catch (NullPointerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get Devices from SQL: %s ", e));
			genErrorMessage(e.getMessage() + " " + e.toString());
			dataSource.close();
			return 0;
		}
		catch (Error e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get Devices from SQL: %s ", e));
			genErrorMessage(e.getMessage() + " " + e.toString());
			dataSource.close();
			return 0;
		}
		catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get Devices from SQL: %s ", e));
			genErrorMessage(e.getMessage() + " " + e.toString());
			dataSource.close();
			return 0;
		}
		finally
		{
			dataSource.close();
			//return 0;
		}
		
		dataSource.close();
		
		logger.info( String.format("***Sended to Exchange messages: %d ***", events));
		
		//removeLineFromFile("sendedEvents.dat", "Tgc1-1Cp1_ping_OK");
	
        return 1;
	}
	
	private List<HashMap<String, Object>> getAllDevices(BasicDataSource dataSource) throws SQLException, Throwable {
		// TODO Auto-generated method stub
	    
		List<HashMap<String,Object>> list = new ArrayList<HashMap<String,Object>>();
		
	    Connection con = null; 
	    PreparedStatement pstmt;
	    ResultSet resultset = null;
	    
	    logger.info(" **** Try to receive all Nodes with units ***** " );
	    try {
	    	con = (Connection) dataSource.getConnection();
			//con.setAutoCommit(false);
			
	    	pstmt = con.prepareStatement("select t1.*,        cast (coalesce(nullif(t2.cfgname, ''), '') as text) as cfgname, " +
       "cast (coalesce(nullif(t2.hwname, ''), '') as text) as hwname, " +
      " cast (coalesce(nullif(t2.cfgdescription, ''), '') as text) as cfgdescription, " +
       "cast (coalesce(nullif(t2.manufacturerpn, ''), '') as text) as manufacturerpn, " +
       "cast (coalesce(nullif(t2.manufacturersn, ''), '') as text) as manufacturersn, "
       + "t2.unitid from "
	    			+ "(SELECT nodeid, parentid, objid, name, type, deviceaddr, treehierarchy, "
	    			+ "split_part(treehierarchy, '.', 2) as x, split_part(treehierarchy, '.', 3) as y, "
	    			+ "split_part(treehierarchy, '.', 4) as z, typeclass "
	    			+ "FROM public.node t1 where t1.typeclass in (1,2) "
	    			+ "and t1.type <> '' /*and t2.z = ''*/ "
	    			+ "order by t1.nodeid ) t1 "
	    			+ "left join public.unit t2 on t1.x = t2.neid and t1.y = t2.slot "
	    			+ "and t2.layer = ?");
	                   // +" LIMIT ?;");
	        //pstmt.setString(1, "");
	        pstmt.setInt(1, 0);
	        
	        logger.debug("DB query: " +  pstmt.toString()); 
	        resultset = pstmt.executeQuery();
	        //resultset.get
	        //con.commit();
	        
	        list = convertRStoList(resultset);
	        
	        
	        resultset.close();
	        pstmt.close();
	        
	        if (con != null) con.close();
	        
	        return list;
	        
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			logger.error( String.format("Error while SQL execution: %s ", e));
			
			if (con != null) con.close();
			
			//return null;
			throw e;
	
		} catch (Throwable e) { //send error message to the same queue
			// TODO Auto-generated catch block
			logger.error( String.format("Error while execution: %s ", e));
			//genErrorMessage(e.getMessage());
			// 0;
			throw e;
		} finally {
	        if (con != null) con.close();
	        
	        //return list;
	    }
	
		
	}
	
	private Device genDeviceObj(HashMap<String, Object> alarm) throws SQLException, Throwable {
		// TODO Auto-generated method stub
		Device gendevice = new Device();
		
		gendevice.setName(alarm.get("name").toString());
		gendevice.setSystemName(alarm.get("hwname").toString());
		gendevice.setDeviceType(alarm.get("type").toString());
		gendevice.setModelNumber(alarm.get("manufacturerpn").toString());
		gendevice.setSerialNumber(alarm.get("manufacturersn").toString());
		gendevice.setDescription(alarm.get("cfgdescription").toString());
		gendevice.setId(alarm.get("nodeid").toString());
		gendevice.setParentID(alarm.get("parentid").toString());
		//gendevice.setParentID(node.getCustomAttributes()[0].G.getValue());
		gendevice.setSource(String.format("%s", endpoint.getConfiguration().getSource()));
		//gendevice.set
		
	
		logger.info(gendevice.toString());
		
		return gendevice;
	}

	private List<HashMap<String, Object>> convertRStoList(ResultSet resultset) throws SQLException {
		
		List<HashMap<String,Object>> list = new ArrayList<HashMap<String,Object>>();
		
		try {
			ResultSetMetaData md = resultset.getMetaData();
	        int columns = md.getColumnCount();
	        //result.getArray(columnIndex)
	        //resultset.get
	        logger.debug("DB SQL columns count: " + columns); 
	        
	        //resultset.last();
	        //int count = resultset.getRow();
	        //logger.debug("MYSQL rows2 count: " + count); 
	        //resultset.beforeFirst();
	        
	        int i = 0, n = 0;
	        //ArrayList<String> arrayList = new ArrayList<String>(); 
	
	        while (resultset.next()) {              
	        	HashMap<String,Object> row = new HashMap<String, Object>(columns);
	            for(int i1=1; i1<=columns; ++i1) {
	            	logger.debug("DB SQL getColumnLabel: " + md.getColumnLabel(i1)); 
	            	logger.debug("DB SQL getObject: " + resultset.getObject(i1)); 
	                row.put(md.getColumnLabel(i1),resultset.getObject(i1));
	            }
	            list.add(row);                 
	        }
	        
	        return list;
	        
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
			return null;

		} finally {

		}
	}

	private BasicDataSource setupDataSource() {
		
		String url = String.format("jdbc:postgresql://%s:%s/%s",
		endpoint.getConfiguration().getPostgresql_host(), endpoint.getConfiguration().getPostgresql_port(),
		endpoint.getConfiguration().getPostgresql_db());
		
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUsername( endpoint.getConfiguration().getUsername() );
        ds.setPassword( endpoint.getConfiguration().getPassword() );
        ds.setUrl(url);
              
        return ds;
    }
	
	
	public static String setRightSeverity(String severity)
	{
		String newseverity = "";
			
		switch (severity) {
        	case "0":  newseverity = PersistentEventSeverity.CRITICAL.name();break;
        	case "1":  newseverity = PersistentEventSeverity.MAJOR.name();break;
        	case "2":  newseverity = PersistentEventSeverity.MINOR.name();break;
        	case "3":  newseverity = PersistentEventSeverity.INFO.name();break;
        	default: newseverity = PersistentEventSeverity.INFO.name();break;

        	
		}
		/*
		System.out.println("***************** colour: " + colour);
		System.out.println("***************** newseverity: " + newseverity);
		*/
		return newseverity;
	}
	
	
}