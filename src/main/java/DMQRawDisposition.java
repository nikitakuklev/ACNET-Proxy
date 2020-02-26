// $Id: DMQRawDisposition.java,v 1.2 2014/12/22 19:09:00 patrick Exp $
//
// Copyright 2002, Universities Research Association. All rights reserved.
//


import gov.fnal.controls.daq.callback.DaqDataCallback;
import gov.fnal.controls.daq.callback.ReadingObjectCallback;
import gov.fnal.controls.daq.callback.SettingObjectCallback;
import gov.fnal.controls.daq.context.CollectionContext;
import gov.fnal.controls.daq.datasource.QueuedRawDataEngineDisposition;
import gov.fnal.controls.daq.items.WhatDaq;
import gov.fnal.controls.daq.scaling.BasicStatusScaling;
import gov.fnal.controls.daq.scaling.GenericRMITransferValueScalable;
import gov.fnal.controls.daq.scaling.ReadSetScaling;
import gov.fnal.controls.daq.schedulers.DataScheduler;

import java.util.Date;

/**
 * Disposition to handle raw data for DMQ
 * 
 */

public class DMQRawDisposition extends QueuedRawDataEngineDisposition {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    transient DataScheduler scheduler = null;
    DaqDataCallback callback;

    /**
     * Create an Multiwire job
     * 
     * @param callback
     *                   Callback from DMQ that will transmit data to client via RabbitMQ
     * @param onChange 	 On Change parameter for MonitorChangeDisposition. Actually ignored.
     * @exception Exception
     */
    public DMQRawDisposition(DaqDataCallback callback, boolean onChange)
    {
	super(false);
	this.callback = callback;

    }

    /**
     * Receive notification of scheduler start in order to setup
     * start of job stuff on server side
     * 
     * @param sch    The scheduler
     */
    public void schedulerBegin(DataScheduler sch)
    {
        super.schedulerBegin(sch);
        scheduler = sch;
    }


    /**
     * Receive queued raw data.
     *
     * Called when an element of this disposition has been received.
     * @param whatDaq   the device.
     * @param error     the ACNET standard error code.
     * @param data      the data array.
     * @param timestamp the time of data collection.
     * @param context   the collection context.
     * @param isChanged new data or error.
     */
    public void queuedRawDataDelivery(WhatDaq whatDaq, int error, byte[] data, Date timestamp, CollectionContext context, boolean isChanged)
    {
        GenericRMITransferValueScalable pdb = whatDaq.getScaling();
        
        // Construct an array of int, short based on the scale length
        // If the scale length is not 2 or 4, just send back array of bytes
        
        int scaleLength;
        if (pdb instanceof ReadSetScaling) {
        	scaleLength = ((ReadSetScaling)pdb).pdulen();
        } else if (pdb instanceof BasicStatusScaling) {
        	scaleLength = 2;
        } else {
        	scaleLength = 4;
        }
        
        Object returnData = null;
        if (scaleLength == 2) {
        	short[] shortData = new short[data.length/2];
            for (int i = 0; i < shortData.length; i++) {
               short MSB = (short) data[2*i+1];
               short LSB = (short) data[2*i];
               shortData[i] = (short) (MSB << 8 | (255 & LSB));
            }
        	returnData = shortData;
        } else if (scaleLength == 4) {
        	int[] intData = new int[data.length/4];
        	for (int i = 0; i < intData.length; i++) {
        		int work;
        		int word = (((int) data[4*i]) & 0xff);
        		work = (((int) data[4*i+1]) & 0xff);
        		word |= (work << 8);
        		work = (((int) data[4*i+2]) & 0xff);
        		word |= (work << 16);
        		work = (((int) data[4*i+3]) & 0xff);
        		word |= (work << 24);
        		intData[i] = word;
        	}
        	returnData = intData;
        } else {
        	returnData = new byte[data.length];
        	System.arraycopy(data, 0, returnData, 0, data.length);
        }
        
        // Callback the client method to process the data
        
        if (callback != null) {
        	if (callback instanceof ReadingObjectCallback) { 
        		ReadingObjectCallback oCallback = (ReadingObjectCallback) callback;
        		oCallback.readingObject(whatDaq, 0, error, timestamp, context, returnData);
        	} else if (callback instanceof SettingObjectCallback) {
        		SettingObjectCallback oCallback = (SettingObjectCallback) callback;
        		oCallback.settingObject(whatDaq, 0, error, timestamp, context, returnData);
        	}
        }
    }
}

