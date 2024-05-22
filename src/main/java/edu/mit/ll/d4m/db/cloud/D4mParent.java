package edu.mit.ll.d4m.db.cloud;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.apache.log4j.Logger;
//import org.apache.log4j.xml.DOMConfigurator;

public abstract class D4mParent {
	private static final Logger log = LoggerFactory.getLogger(D4mParent.class);

  // DH2015: This is redundant with D4mConfig

/** 
	static {
    try {
      DOMConfigurator.configure(D4mParent.class.getClassLoader().getResource("log4j.xml"));
    } catch (Exception e) {
      log.warn("problem loading log4j",e);
    }
	}
*/
  /** Kept for compatibility with older D4M. */
  @Deprecated
	public void setCloudType(String cloudType) {
	}

}
/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% D4M: Dynamic Distributed Dimensional Data Model
% MIT Lincoln Laboratory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% (c) <2010> Massachusetts Institute of Technology
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 */
