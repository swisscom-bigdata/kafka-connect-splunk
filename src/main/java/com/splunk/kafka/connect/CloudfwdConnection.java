/*
 * Proprietary and confidential. Copyright Splunk 2015
 */
package com.splunk.kafka.connect;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.Connections;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ghendrey
 */
public class CloudfwdConnection {
    private static final Logger log = LoggerFactory.getLogger(CloudfwdConnection.class);
    public static Connection create(SplunkSinkConnectorConfig cfg, ConnectionCallbacks cb) throws IOException{
        Properties p = getCloudfwdDotProperties();
        Connection conn = Connections.create(cb, p); 
        StringBuilder uris = new StringBuilder();       
        conn.getSettings().setUrls(makeCommaSeparatedList(cfg, uris));
        conn.getSettings().setToken(cfg.getHecConfig().getToken());        
        return conn;
    }

    private static Properties getCloudfwdDotProperties() throws IOException{
        Properties p = new Properties();
        try(InputStream is = Connection.class.getResourceAsStream("/cloudfwd.properties");){
            if(null != is){
                p.load(is);
            }else{
                log.error("failed to open cloudfwd.properties");
            }
        }        
        return p;
        
    }
    private static String makeCommaSeparatedList(SplunkSinkConnectorConfig cfg,
            StringBuilder uris) {
        //make comma separated list
        for (Iterator<String> it = cfg.getHecConfig().getUris().iterator(); it.hasNext();){
            uris.append(it.next());
            if(it.hasNext()){
                uris.append(',');
            }
        }
        return uris.toString();
    }
    
    
}
