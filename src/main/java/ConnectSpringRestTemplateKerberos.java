import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.kerberos.client.KerberosRestTemplate;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple example usage for connecting Spring ReST Template to MarkLogic using Kerberos Authentication
 */
public class ConnectSpringRestTemplateKerberos {

    private static Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(String[] args) {

        /* System.setProperty("sun.security.krb5.debug", "true");
        System.setProperty("sun.security.spnego.debug", "true"); */

        LOG.info("Spring ReST Template Kerberos Connection Test...");

        Map<String, Object> loginOptions = new HashMap<>();
        loginOptions.put("debug", "true");

        KerberosRestTemplate restTemplate =
                new KerberosRestTemplate(null, "-", loginOptions);

        LOG.info(String.format("Response: %s", restTemplate.getForEntity("http://localhost/", String.class)));

    }
}