package com.gvenzl;

import oracle.security.pki.OracleWallet;
import oracle.security.pki.textui.OraclePKIGenFunc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class CloudConnectionManager {

    public static Connection getConnection(File fUrl,String user,String password, String serviceName, String ) throws IOException, SQLException{
        Path tmpDir = Files.createTempDirectory("oracle_cloud_config");
    
        Path tmpZip = tmpDir.resolve("temp.zip");
        Files.copy(new FileInputStream(fUrl), tmpZip);
    
        ZipFile zf = new ZipFile(tmpZip.toFile());
    
        Enumeration<? extends ZipEntry> entities = zf.entries();
        while (entities.hasMoreElements()) {
            ZipEntry entry = entities.nextElement();
            String name = entry.getName();
            Path p = tmpDir.resolve(name);
            Files.copy(zf.getInputStream(entry), p);
        }
    
        String pathToWallet = tmpDir.toFile().getAbsolutePath();
    

        System.setProperty ("oracle.net.tns_admin", pathToWallet);
    
        System.setProperty ("oracle.net.ssl_server_dn_match", "true");
        System.setProperty ("oracle.net.ssl_version", "1.2");
    
        // open the CA's wallet
        OracleWallet caWallet = new OracleWallet();
        caWallet.open(pathToWallet, null);
    
        String passwd = generateRandomSecurePassword();
        char[] keyAndTrustStorePasswd = OraclePKIGenFunc.getCreatePassword(passwd, false);
            
        // certs
        OracleWallet jksK = caWallet.migratePKCS12toJKS(keyAndTrustStorePasswd, OracleWallet.MIGRATE_KEY_ENTIRES_ONLY);
    
        // migrate (trusted) cert entries from p12 to different jks store
        OracleWallet jksT = caWallet.migratePKCS12toJKS(keyAndTrustStorePasswd, OracleWallet.MIGRATE_TRUSTED_ENTRIES_ONLY);
        String trustPath = pathToWallet+ "/trustStore.jks";
        String keyPath = pathToWallet+ "/keyStore.jks";
        jksT.saveAs(trustPath);
        jksK.saveAs(keyPath);
    
        System.setProperty("javax.net.ssl.trustStore",trustPath);
        System.setProperty("javax.net.ssl.trustStorePassword",passwd);
        System.setProperty("javax.net.ssl.keyStore",keyPath);
        System.setProperty("javax.net.ssl.keyStorePassword",passwd);
    
        Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@" + serviceName, user,password);
        zf.close();
        deleteFile(tmpDir.toFile());
        return conn;
     }
     
    private static String generateRandomSecurePassword() {
        return new BigInteger(130, new SecureRandom()).toString(32);
    }

    private static void deleteFile(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                deleteFile(c);
        }
        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }

    private static void setProxy(String path, String proxy) {
        Path file = Paths.get( ( path + File.separatorChar + "tnsnames.ora"));
        Charset charset = StandardCharsets.UTF_8;

        String[] proxy= proxyHostPort.split(":");

        String content = new String(Files.readAllBytes(file), charset);
        content = content.replaceAll("address\\=\\(protocol=tcps\\)", "address=(https_proxy="+proxy[0]+")(https_proxy_port="+(proxy.length==2?proxy[1]:"80")+")(protocol=tcps)");
        Files.write(file, content.getBytes(charset));
    }
}
