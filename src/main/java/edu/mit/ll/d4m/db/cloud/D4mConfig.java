/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import org.apache.log4j.ConsoleAppender;
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//import org.apache.log4j.PropertyConfigurator;

//import org.apache.logging.log4j.core.appender.ConsoleAppender;
//import org.apache.logging.log4j.Level;
//import org.apache.logging.log4j.Logger;
//import org.apache.logging.log4j.PropertyConfigurator;

import java.net.URL;
/**
 * D4M configuration
 * @author CHV8091
 *
 */
public class D4mConfig {
	private static final Logger log = LoggerFactory.getLogger(D4mConfig.class);

	public static final String ACCUMULO="Accumulo";
	public static boolean DEBUG;

  // DH2015: This is redundant with D4mParent
/** 
	static {
		ClassLoader clsloader = D4mConfig.class.getClassLoader();
		URL urlLog4j = clsloader.getResource("log4j.xml");
		if(urlLog4j != null) {
//			System.out.println("D4mConfig::LOG4J file path = "+urlLog4j.getPath());
			PropertyConfigurator.configure(urlLog4j);

		}
		else {
			System.err.println("No file log4j.xml in path");

			ConsoleAppender ca = new ConsoleAppender();
			ca.setThreshold(Level.WARN);
			org.apache.log4j.BasicConfigurator.configure(ca);
		}
		String tmpDebug = System.getProperty("d4m.debug", "false");
		DEBUG = Boolean.parseBoolean(tmpDebug);
	}
	*/
}
