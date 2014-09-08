package satisfaction.retry;

import java.lang.annotation.*;

import org.joda.time.Duration;
/**
 * 
 *   Annotation on a Goal to say 
 *    that the goal should be retried
 *   
 *   Needs to be implemented as a
 *    Java Annotation, because
 *    Scala ClassfileAnnotation doesn't
 *    have runtime retention 
 *
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Retry {
    int numRetries = 3;
    Duration waitPeriod  = Duration.standardSeconds(30);

}
