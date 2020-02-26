//  (c) 2010 Fermi Research Alliance
//  $Id: AcnetJobFactoryMBean.java,v 1.2.2.1 2011/03/17 20:39:27 apetrov Exp $


/**
 * Public management interface for a factory creating ACNET data acquisition jobs.
 *
 * @author Andrey Petrov
 */
public interface AcnetJobFactoryMBean {

    /**
     * Gets the purge rate.
     * 
     * @return the purge rate in milliseconds.
     * @see #setPurgeRate(int) 
     */
    int getPurgeRate();

    /**
     * Returns the number of ACNET DAQ users currently held by this factory.
     * <p>
     * Each <code>DaqUser</code> instance corresponds to one or more DMQ
     * clients connected to this data provider. Unneeded instances are removed
     * by {@link #purge()}.
     *
     * @return the number of DAQ users.
     * @see gov.fnal.controls.daq.acquire.DaqUser
     */
    int getDaqUserCount();

    /**
     * Removes unneeded DAQ jobs and DAQ users held by this data provider.
     * <p>
     * This operation is normally called by an internal periodic task at a
     * rate specified via {@link #setPurgeRate(int)}.
     *
     * @return the number of evicted DAQ jobs.
     * @see #setPurgeRate(int) 
     */
    int purge();

}
