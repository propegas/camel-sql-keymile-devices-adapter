package ru.atc.camel.keymile.devices;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.ScheduledPollConsumer;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.atc.adapters.type.Device;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static ru.atc.adapters.message.CamelMessageManager.genAndSendErrorMessage;

public class KeymileConsumer extends ScheduledPollConsumer {

    private static final Logger logger = LoggerFactory.getLogger("mainLogger");
    private static final Logger loggerErrors = LoggerFactory.getLogger("errorsLogger");
    private static KeymileEndpoint endpoint;
    private String parentNodeGroupHash = "";

    public KeymileConsumer(KeymileEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        KeymileConsumer.endpoint = endpoint;
        this.setTimeUnit(TimeUnit.MINUTES);
        this.setInitialDelay(0);
        this.setDelay(endpoint.getConfiguration().getDelay());
    }

    private static String hashString(String message, String algorithm)
            throws Exception {

        try {
            MessageDigest digest = MessageDigest.getInstance(algorithm);
            byte[] hashedBytes = digest.digest(message.getBytes("UTF-8"));

            return convertByteArrayToHexString(hashedBytes);
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException ex) {
            throw new RuntimeException(
                    "Could not generate parentNodeGroupHash from String", ex);
        }
    }

    //CHECKSTYLE.OFF: MagicNumber
    private static String convertByteArrayToHexString(byte[] arrayBytes) {
        StringBuilder stringBuffer = new StringBuilder();
        for (byte arrayByte : arrayBytes) {
            stringBuffer.append(Integer.toString((arrayByte & 0xff) + 0x100, 16)
                    .substring(1));
        }
        return stringBuffer.toString();
    }
    //CHECKSTYLE.ON: MagicNumber

    @Override
    protected int poll() throws Exception {

        String operationPath = endpoint.getOperationPath();

        if (java.util.Objects.equals(operationPath, "devices"))
            return processSearchDevices();

        // only one operation implemented for now !
        throw new IllegalArgumentException("Incorrect operation: " + operationPath);
    }

    @Override
    public long beforePoll(long timeout) throws Exception {

        logger.info("*** Before Poll!!!");

        return timeout;
    }

    private void genErrorMessage(String message, Exception exception) {
        genAndSendErrorMessage(this, message, exception,
                endpoint.getConfiguration().getAdaptername());
    }

    private int processSearchDevices() {

        BasicDataSource dataSource = setupDataSource();

        List<HashMap<String, Object>> listNodeGroups;
        List<HashMap<String, Object>> listAllDevices;

        int events = 0;
        try {

            // generate NodeGroup
            logger.info("***Try to generate NodeGroup***");
            listNodeGroups = getNodeGroups();

            // get All Nodes (units)
            logger.info("***Try to get All Nodes***");
            listAllDevices = getAllDevices(dataSource);
            logger.info(String.format("***Received %d Devices from SQL***", listAllDevices.size()));

            // add NodeGroup
            listAllDevices.addAll(listNodeGroups);

            events = genAndProcessDevices(listAllDevices);

            dataSource.close();

        } catch (Exception e) {
            genErrorMessage("Error while get Devices from DB", e);
            return 0;
        } finally {
            closeConnectionsAndDS(dataSource);
        }

        logger.info(String.format("***Sended to Exchange messages: %d ***", events));

        return 1;
    }

    private void closeConnectionsAndDS(BasicDataSource dataSource) {
        try {
            dataSource.close();
        } catch (SQLException e) {
            logger.error("Error while closing Datasource", e);
        }
    }

    private int genAndProcessDevices(List<HashMap<String, Object>> listAllDevices) {
        String type;
        String id;
        int events = 0;

        Device gendevice;

        for (int i = 0; i < listAllDevices.size(); i++) {

            type = listAllDevices.get(i).get("type").toString();
            id = listAllDevices.get(i).get("nodeid").toString();
            logger.debug("DB row " + i + ": " + type +
                    " " + id);

            gendevice = genDeviceObj(listAllDevices.get(i));

            logger.debug("*** Create Exchange ***");

            String key = gendevice.getId() + "_" +
                    gendevice.getName();

            Exchange exchange = getEndpoint().createExchange();
            exchange.getIn().setBody(gendevice, Device.class);
            exchange.getIn().setHeader("DeviceId", key);
            exchange.getIn().setHeader("DeviceType", gendevice.getDeviceType());
            exchange.getIn().setHeader("ParentId", gendevice.getParentID());

            try {
                getProcessor().process(exchange);
                events++;
            } catch (Exception e) {
                genErrorMessage("Error while process Exchange message ", e);
            }

        }

        logger.info(String.format("***Received %d Keymile Devices from SQL*** ", listAllDevices.size()));
        return events;
    }

    private List<HashMap<String, Object>> getNodeGroups() {
        List<HashMap<String, Object>> list = new ArrayList<>();

        logger.info(" **** Try to generate main service NodeGroup ***** ");

        try {

            String nodeGroupName = endpoint.getConfiguration().getServiceNodeGroup();

            HashMap<String, Object> row = new HashMap<>(5);

            logger.debug(String.format("*** Trying to generate parentNodeGroupHash for main service NodeGroup with Pattern: %s",
                    nodeGroupName));

            parentNodeGroupHash = hashString(String.format("%s", nodeGroupName), "SHA-1");

            logger.debug("*** Generated Hash: " + parentNodeGroupHash);

            String newHostgroupName;
            String service;

            // Example group :
            // (Невский.СЭ)ТЭЦ-1
            Pattern p = Pattern.compile("\\((.*)\\)(.*)");
            Matcher matcher = p.matcher(nodeGroupName);

            // if nodegroup has Group pattern
            if (matcher.matches()) {
                logger.debug("*** Finded NodeGroup with Pattern: " + nodeGroupName);

                newHostgroupName = matcher.group(2);
                service = matcher.group(1);

                logger.debug("*** newHostgroupName: " + newHostgroupName);
                logger.debug("*** service: " + service);

            } else
                return Collections.emptyList();

            row.put("id", parentNodeGroupHash);
            row.put("name", newHostgroupName);
            row.put("type", "NodeGroup");
            row.put("nodeid", parentNodeGroupHash);
            row.put("service", service);

            logger.info(row.toString());

            list.add(row);

        } catch (Exception ex) {
            throw new RuntimeException("Ошибка при запросе и формировании групп", ex);
        }
        return list;
    }

    private List<HashMap<String, Object>> getAllDevices(BasicDataSource dataSource) throws Exception {

        List<HashMap<String, Object>> list;

        Connection con = null;
        PreparedStatement pstmt;
        ResultSet resultset;

        logger.info(" **** Try to receive all Nodes with units ***** ");
        try {
            con = dataSource.getConnection();

            pstmt = con.prepareStatement(new StringBuilder()
                    .append("select t1.*, cast (coalesce(nullif(t2.cfgname, ''), '') as text) as cfgname, ")
                    .append("cast (coalesce(nullif(t2.hwname, ''), '') as text) as hwname, ")
                    .append("cast (coalesce(nullif(t2.cfgdescription, ''), '') as text) as cfgdescription, ")
                    .append("cast (coalesce(nullif(t2.manufacturerpn, ''), '') as text) as manufacturerpn, ")
                    .append("cast (coalesce(nullif(t2.manufacturersn, ''), '') as text) as manufacturersn, ")
                    .append("t2.unitid from ")
                    .append("(SELECT nodeid, parentid, objid, name, type, deviceaddr, treehierarchy, ")
                    .append("split_part(treehierarchy, '.', 2) as x, split_part(treehierarchy, '.', 3) as y, ")
                    .append("split_part(treehierarchy, '.', 4) as z, typeclass ")
                    .append("FROM public.node t1 where t1.typeclass in (1,2) ")
                    .append("and t1.type <> '' /*and t2.z = ''*/ ")
                    .append("order by t1.nodeid ) t1 ")
                    .append("left join public.unit t2 on t1.x = t2.neid and t1.y = t2.slot ")
                    .append("and t2.layer = ?")
                    .toString());

            pstmt.setInt(1, 0);

            logger.debug("DB query: " + pstmt.toString());
            resultset = pstmt.executeQuery();

            list = convertRStoList(resultset);

            resultset.close();
            pstmt.close();

            con.close();

            return list;

        } catch (Exception e) {
            throw new RuntimeException("Ошибка при операции с БД", e);
        } finally {
            if (con != null)
                con.close();
        }

    }

    private Device genDeviceObj(HashMap<String, Object> alarm) {

        Device gendevice = new Device();

        // if NodeGroup
        if (alarm.containsKey("id") && alarm.get("id").toString().equals(parentNodeGroupHash)) {
            gendevice.setId(parentNodeGroupHash);
            gendevice.setName(String.format("%s", alarm.get("name").toString()));
            gendevice.setService(String.format("%s", alarm.get("service").toString()));
            gendevice.setDeviceType("NodeGroup");
        } else {
            gendevice.setName(alarm.get("name").toString());
            gendevice.setHostName(alarm.get("hwname").toString());
            gendevice.setDeviceType(alarm.get("type").toString());
            gendevice.setModelName(alarm.get("manufacturerpn").toString());
            gendevice.setSerialNumber(alarm.get("manufacturersn").toString());
            //gendevice.setDescription(alarm.get("cfgdescription").toString());
            gendevice.setId(String.format("%s:%s", endpoint.getConfiguration().getSource(),
                    alarm.get("nodeid").toString()));

            String parentid = alarm.get("parentid").toString();
            if ("0".equals(parentid))
                parentid = null;
            else
                parentid = String.format("%s:%s", endpoint.getConfiguration().getSource(), parentid);

            gendevice.setParentID(parentid);
            //set main service NodeGroup as a parent for all node
            if (gendevice.getParentID() == null || gendevice.getParentID().isEmpty())
                gendevice.setParentID(parentNodeGroupHash);
        }

        gendevice.setSource(String.format("%s", endpoint.getConfiguration().getSource()));

        logger.info(gendevice.toString());

        return gendevice;
    }

    private List<HashMap<String, Object>> convertRStoList(ResultSet resultset) throws SQLException {

        List<HashMap<String, Object>> list = new ArrayList<>();

        try {
            ResultSetMetaData md = resultset.getMetaData();
            int columns = md.getColumnCount();

            logger.debug("DB SQL columns count: " + columns);

            while (resultset.next()) {
                HashMap<String, Object> row = new HashMap<>(columns);
                for (int i1 = 1; i1 <= columns; ++i1) {
                    logger.debug("DB SQL getColumnLabel: " + md.getColumnLabel(i1));
                    logger.debug("DB SQL getObject: " + resultset.getObject(i1));
                    row.put(md.getColumnLabel(i1), resultset.getObject(i1));
                }
                list.add(row);
            }

            return list;

        } catch (SQLException e) {
            genErrorMessage("Ошибка конвертирования результата запроса", e);
            return Collections.emptyList();
        }
    }

    private BasicDataSource setupDataSource() {

        String url = String.format("jdbc:postgresql://%s:%s/%s",
                endpoint.getConfiguration().getPostgresqlHost(), endpoint.getConfiguration().getPostgresqlPort(),
                endpoint.getConfiguration().getPostgresqlDb());

        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setMaxIdle(10);
        ds.setMinIdle(5);
        ds.setUsername(endpoint.getConfiguration().getUsername());
        ds.setPassword(endpoint.getConfiguration().getPassword());
        ds.setUrl(url);

        return ds;
    }

}