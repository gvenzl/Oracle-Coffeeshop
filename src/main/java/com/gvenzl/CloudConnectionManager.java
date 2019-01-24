package com.gvenzl;

import oracle.security.pki.OracleWallet;
import oracle.security.pki.textui.OraclePKIGenFunc;

import javax.crypto.Cipher;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class CloudConnectionManager {

	 public static Connection getConnection(File fUrl,String user,String password, String serviceName) throws IOException, SQLException{
		    Path tmp = Files.createTempDirectory("oracle_cloud_config");
		    // clean up on exit
		    tmp.toFile().deleteOnExit();

		    Path pzip = tmp.resolve("temp.zip");
		    Files.copy(new FileInputStream(fUrl), pzip);

		    ZipFile zf = new ZipFile(pzip.toFile());
		    Enumeration<? extends ZipEntry> entities = zf.entries();
		    while (entities.hasMoreElements()) {
		        ZipEntry entry = entities.nextElement();
		        String name = entry.getName();
		        Path p = tmp.resolve(name);
		        Files.copy(zf.getInputStream(entry), p);
		    }

		    String pathToWallet = tmp.toFile().getAbsolutePath();


		    System.setProperty ("oracle.net.tns_admin", pathToWallet);

		    System.setProperty ("oracle.net.ssl_server_dn_match", "true");
		    System.setProperty ("oracle.net.ssl_version", "1.2");

		    // open the CA's wallet
		    OracleWallet caWallet = new OracleWallet();
		    caWallet.open(pathToWallet, null);

		    String passwd = generateRandomSecurePassword();
		    char[] keyAndTrustStorePasswd = OraclePKIGenFunc.getCreatePassword(passwd, false);
		    
		    
		    // Leave for JDK stuff UJE
		    int maxKeySize = 0;
		    try {
		       maxKeySize = Cipher.getMaxAllowedKeyLength("AES");

		    } catch (NoSuchAlgorithmException e) {
		    	System.out.println("Bad stuff");
		    }
		    
		    if (maxKeySize <=128 ) {
		    	System.out.println("bad stuff");
		    }
		    // END ------
		    
		    
		    
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
		    return conn;
	 }
	 
	 private static String generateRandomSecurePassword() {
	     return new BigInteger(130, new SecureRandom()).toString(32);
	 }
}
