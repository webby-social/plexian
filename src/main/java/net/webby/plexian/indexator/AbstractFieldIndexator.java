package net.webby.plexian.indexator;

import net.webby.plexian.Plexian;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class AbstractFieldIndexator implements Runnable {

	private final Log log = LogFactory.getLog(getClass());
	
	protected String field;
	protected Plexian plexian;
	private volatile boolean running = false;
	
	protected abstract void runInternal();
	
	public void run() {
		
		if (!running) {
	
			running = true;
			
			try {
				runInternal();
			}
			catch(Exception e) {
				log.error("indexator fail", e);
			}
			
			running = false;
		}
	}

	public boolean isRunning() {
		return running;
	}

	public String getField() {
	    return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public void setPlexian(Plexian plexian) {
		this.plexian = plexian;
	}
}
