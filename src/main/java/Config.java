//  (c) 2010 Fermi Research Alliance
//  $Id: Config.java,v 1.2 2011/02/03 21:37:21 apetrov Exp $


import gov.fnal.controls.service.dmq.impl.ConfigBase;

/**
 * Configuration for the ACNET data provider.
 * 
 * @author Andrey Petrov
 */
final class Config extends ConfigBase {

    static final int SERVER_PURGE_RATE = load( "dmq.server.purge-rate", 60000 );

    static final String MBEAN_DOMAIN = "gov.fnal.controls.service.dmq";

    private Config() {}

}