package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.rules.ExternalResource;

public class RealAccumuloTester extends ExternalResource implements IAccumuloTester {
    private static final Logger log = LoggerFactory.getLogger(RealAccumuloTester.class);

    private ClientConfiguration cc;
    private String username = "root";
    private PasswordToken auth;

    private Instance instance;

    public RealAccumuloTester(String instanceName, String zookeeperHost,
                              String username, PasswordToken auth) {
        cc = ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zookeeperHost); // .withZkTimeout(timeout)
        this.username = username;
        this.auth = auth;
    }

    public Connector getConnector() {
        Connector c;

        try {
            c = instance.getConnector(username, auth);
        } catch (AccumuloException | AccumuloSecurityException e) {
            log.error("failed to connect to Accumulo instance "+instance.getInstanceName(),e);
            throw new RuntimeException(e);
        }
        return c;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public PasswordToken getPassword() {
        return auth;
    }

    @Override
    protected void before() throws Throwable {
//        instance = new ZooKeeperInstance(cc.get(ClientConfiguration.ClientProperty.INSTANCE_NAME),
//                                    cc.get(ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST));
        instance = new ZooKeeperInstance(cc);
        log.debug("setUp ok - ClientConfiguration="+cc+" instance=" + instance.getInstanceName());
    }

//    @Override
//    protected void after() {
//        //log.debug("tearDown ok - instance destroyed");
//    }
}
