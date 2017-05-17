package org.mycompany.consumer;

public abstract class Consumer {
	
	protected abstract void setupConsumer();
	protected abstract void closeConsumption();
	protected abstract void doComsumption();

	public void consume() {
		setupConsumer();
		doComsumption();
        closeConsumption();
	}
		
}
