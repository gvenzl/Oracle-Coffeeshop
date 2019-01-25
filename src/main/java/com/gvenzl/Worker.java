package com.gvenzl;

import com.gvenzl.data.Coffee;
import com.gvenzl.data.CustomerAndLocation;
import com.gvenzl.data.StaticData;
import oracle.jdbc.pool.OracleDataSource;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;

public class Worker implements Runnable {

    private final Properties props;

    private Connection conn;
    
    private int batchSize=0;
    private boolean stop=false;
    private boolean loadDB=false;
    private boolean loadREST;
    private boolean writeFile=false;


    private PreparedStatement stmt=null;
    private Random random;
    private BufferedWriter bw;
    
    private static final int MAX_ORDERS=5;
    
    public Worker(Properties props)
                    throws SQLException, IOException, IllegalArgumentException {

        this.props = props;
        this.random = new Random();
        
        if (!get("outputFileName").isEmpty()) {
            writeFile = true;
            
            try {
                bw = new BufferedWriter(new FileWriter(get("outputFileName")));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        if (!get("jdbcURL").isEmpty() || !get("tnsName").isEmpty()) {
            loadDB = true;

            String url = get("jdbcURL");
            if (url.isEmpty()) {
                url = get("tnsName");
            }
            if (!get("cloudCredentialsFile").isEmpty()) {
                conn = CloudConnectionManager.getConnection(
                        new File(get("cloudCredentialsFile")),
                            get("username"), get("password"), url);
                System.out.println("Connected to Cloud Database.");
            }
            else {
                OracleDataSource ods = new OracleDataSource();
				ods.setUser(get("username"));
				ods.setPassword(get("password"));
                ods.setURL("jdbc:oracle:thin:@" + url);
                conn = ods.getConnection();
                System.out.println("Connected to Oracle Database.");
            }
            conn.setAutoCommit(false);
            stmt = conn.prepareStatement(
                    "INSERT INTO " + get("tableName") + " (" + get("tableColumnName") + ") VALUES(?)");
        }
        
        loadREST = !get("restURL").isEmpty();

        if (!writeFile && !loadDB && !loadREST) {
            throw new IllegalArgumentException("No load instructions have been passed on. " +
                    "Please specify either a REST URL, JDBC URL, TNS name, or file location.");
        }
    }
    
    private String generateDate() {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        df.setTimeZone(tz);
        return " \"date\": \"" + df.format(getDate()) + "\"";
    }
    
    private Date getDate() {

        String historicData = get("historicData");
        if (historicData.equalsIgnoreCase("true") || historicData.equalsIgnoreCase("yes")) {
            return generateHistoricDate();
        }
        else {
            return getCurrentDate();
        }
    }
    
    private Date getCurrentDate() {
        return new Date();
    }
    
    private Date generateHistoricDate() {
        return new Date(new Date().getTime() - (random.nextInt(365) *24 *3600 * 1000L));
    }
    
    private String generateLocation() {
        return CustomerAndLocation.getCustomerAndLocation();
    }
    
    private String generateStatic() {
        String sd = get("staticData");
        boolean generate = sd.equalsIgnoreCase("true") || sd.equalsIgnoreCase("yes");
        return StaticData.getStaticData(generate);
    }
    
    private String generateOrders() {
        
        double salesTotal = 0.0;
        Coffee coffeeSale = new Coffee();
        
        // Get random number of orders
        int orders = new Random().nextInt(MAX_ORDERS) + 1;

        StringBuilder orderBuilder = new StringBuilder("\"order\": [");
        for (int i = 0; i<orders; i++) {
            Coffee.CoffeeEntry coffee = coffeeSale.getCoffee();
            orderBuilder.append(coffee.coffee).append(",");
            salesTotal += coffee.salesAmount;
        }

        String order = orderBuilder.toString();

        // Round to 2 digits after the comma
        DecimalFormat df = new DecimalFormat("###.##");
        
        return  "  \"salesAmount\": " + df.format(salesTotal) + ",\n  " +
                      order.substring(0, order.length()-1) + "]";
    }
    
    private String generateSale() {
        return "{\n" +
                generateStatic() + ",\n" +
                generateDate() + ",\n" +
                generateLocation() + ",\n" +
                generateOrders() + "\n}";
    }
    
    @Override
    public void run() {
        Random random = new Random();
        int waitSec = Integer.valueOf(get("waitInSeconds"));
        while (!stop) {
            loadData();
            try {
                if (waitSec > 0 ) {
                    int sleep = random.nextInt(waitSec);
                    Thread.sleep(sleep*1000);
                }
            } catch (InterruptedException e) {
                stop=true;
            }
        }
        
        try {
            conn.close();
        } catch (SQLException e) {
            // Ignore SQLException on connection close
        }
    }
    
    private void loadData() {
        String order = generateSale();
        if (writeFile) {
            writeIntoFile(order);
        }
        
        if (loadDB) {
            loadDataIntoDB(order);
        }
        
        if (loadREST) {
            loadRest(order);
        }
    }
    
    private void writeIntoFile(String order) {
        try {
            bw.write(order.replaceAll("\n", "").replaceAll("\r", "").replaceAll("  ", "") + "\n");
        } catch (IOException e) {
            System.out.println("Can't write file.");
            System.out.println(e.getMessage());
        }
    }
    
    private void loadDataIntoDB(String data) {
        try {
            batchSize = batchSize + 1;
            stmt.setBlob(1, new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)));
            stmt.addBatch();
            if (batchSize == Integer.valueOf(get("batchSize"))) {
                stmt.executeBatch();
                conn.commit();
                batchSize = 0;
            }
            
        } catch (SQLException e) {
            System.out.println("Error loading data into the database");
            System.out.println(e.getMessage());
            stop=true;
        }
        
    }
    
    private void loadRest(String data) {
        
        TrustManager[] trustAllCerts = new TrustManager[] { 
                new X509TrustManager() {     
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() { 
                        return new X509Certificate[0];
                    } 
                    public void checkClientTrusted( 
                        java.security.cert.X509Certificate[] certs, String authType) {
                        } 
                    public void checkServerTrusted( 
                        java.security.cert.X509Certificate[] certs, String authType) {
                    }
                } 
            }; 
    
        try {
            SSLContext ctx = SSLContext.getInstance("SSL");
            ctx.init(null, trustAllCerts, new java.security.SecureRandom()); 
            HttpsURLConnection.setDefaultSSLSocketFactory(ctx.getSocketFactory());
            
            CloseableHttpClient client = HttpClients.custom().setSSLContext(ctx).setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE).build();
            HttpPost post = new HttpPost(get("restURL"));
            post.setEntity(
                new StringEntity(data,
                        ContentType.create("application/json", "UTF-8")));

            HttpResponse response = client.execute(post);
            if (response.getStatusLine().getStatusCode() >= 400) {
                System.out.println("REST service not available: " + response.getStatusLine());
            }
            else {
                System.out.println("Data successfully posted!");
            }
        } catch (Exception e) {
            System.out.println("Error on calling REST: " + e.getMessage());
        }
    }

    private String get(String key) {
        return props.getProperty(key);
    }
}
