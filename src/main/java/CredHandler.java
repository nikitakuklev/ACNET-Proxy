import gov.fnal.controls.kerberos.KerberosLoginContext;
import gov.fnal.controls.service.dmq.CredentialHandler;
import gov.fnal.controls.service.dmq.JobManager;
import org.ietf.jgss.GSSException;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import java.util.logging.Logger;

public class CredHandler implements CredentialHandler {
    private static final Logger log = Logger.getLogger(CredHandler.class.getName());
    private final KerberosLoginContext krb5 = KerberosLoginContext.getInstance();

    private Subject sb = null;

    public long last_cred_time = 0;

    public CredHandler(Subject subj) {this.sb = subj;}

    public final synchronized void credentialException(JobManager var1, GSSException var2) {
        if (var1 != null && var2 != null) {
            if (var2.getMajor() == 13 && var2.getMinor() == 0) {
                try {
                    if (System.currentTimeMillis() - this.last_cred_time > 10000) {
                        this.krb5.login();
                        this.sb = this.krb5.getSubject();
                        var1.setCredentials(this.sb);
                        this.last_cred_time = System.currentTimeMillis();
                        log.info(String.format("Authenticated ticket until %s", this.krb5.getTicket().getEndTime()));

                    } else {
                        log.info("Setting cached kerberos subject: " + sb.getPrincipals());
                        var1.setCredentials(sb);
                    }
//                    if (sb != null) {
//                        log.info("Setting cached kerberos subject: " + sb.getPrincipals());
//                        var1.setCredentials(sb);
//                    } else {
//                        this.krb5.login();
//                        var1.setCredentials(this.krb5.getSubject());
//                        sb = this.krb5.getSubject();
//                    }
                    return;
                } catch (LoginException var4) {
                }
            } else {
                log.severe("Weird kerberos exception: " + var2.getMajorString() + "; " + var2.getMinorString());
                log.severe(var2.getMessage());
            }

            if (var2.getMajor() == 13 && var2.getMinor() == -1) {
                if (var2.getMinorString().startsWith("no supported default etypes for default_tgs_enctypes")) {
                    log.severe("No supported Kerberos encryption types found");
                } else {
                    log.severe("Kerberos login failed: " + var2.getMajorString() + "; " + var2.getMinorString());
                }
                //System.exit(-1);
            } else {
                log.severe("The program can not find Kerberos credentials on this computer:\n\n        " + var2.getMessage() + "\n\nIn order to connect to the data acquisition system you must have\neither a valid Kerberos ticket or a permanent keytab available.\n\nPLEASE OBTAIN KERBEROS CREDENTIALS BEFORE PROCEEDING!");
            }

            (new RuntimeException()).printStackTrace();

        } else {
            throw new NullPointerException();
        }
    }
}