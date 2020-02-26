//  (c) 2010 Fermi Research Alliance
//  $Id: SmartDaqUser.java,v 1.2 2011/02/03 21:37:21 apetrov Exp $

import gov.fnal.controls.daq.acquire.DaqUser;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wrapper for <code>DaqUser</code> implementing a user counter.
 *
 * @author Andrey Petrov
 * @see gov.fnal.controls.daq.acquire.DaqUser
 */
class SmartDaqUser {
    
    private final AtomicInteger useCount = new AtomicInteger();
    
    private final DaqUser user;

    SmartDaqUser( DaqUser user ) {
        this.user = user;
    }

    public DaqUser getUser() {
        return user;
    }

    public void incrementUseCount() {
        useCount.incrementAndGet();
    }

    public void decrementUseCount() {
        useCount.decrementAndGet();
    }

    public int getUseCount() {
        return useCount.get();
    }

}
