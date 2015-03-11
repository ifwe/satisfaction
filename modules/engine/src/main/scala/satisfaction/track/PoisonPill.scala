package satisfaction
package track


import java.lang.management._
import javax.management.NotificationListener
import javax.management.Notification
import scala.collection.JavaConversions._
import javax.management.NotificationEmitter
import com.sun.management.HotSpotDiagnosticMXBean
import javax.management.openmbean.CompositeData

/**
 *  Monitor Garbage collection, and die 
 *   if we have run out of PermGen space
 */

class PoisonPill(val thresholdPct : Double, val memPoolName : String) extends Logging {
  
    // This is the name of the HotSpot Diagnostic MBean
    val HOTSPOT_BEAN_NAME = "com.sun.management:type=HotSpotDiagnostic"
    val MIN_RECLAIM_PCT = 0.005

    lazy val  memPoolBean : MemoryPoolMXBean = {
       ManagementFactory.getMemoryPoolMXBeans().filter( _.getName == memPoolName ).get(0)
    }
    
    val notificationListener = new NotificationListener {
       def handleNotification( notification : Notification, handback: Object)  {
          val cd :  CompositeData  =  notification.getUserData().asInstanceOf[CompositeData]
          val memNotifInfo  : MemoryNotificationInfo = MemoryNotificationInfo.from(cd);
          if( memNotifInfo.getPoolName() == memPoolName ) {
             info(s" Received MemoryNotification for Pool ${memNotifInfo.getPoolName()} ${notification.getType()} ${notification.getSequenceNumber()} at${notification.getTimeStamp()} :: ${notification.getMessage()} ")
             info(s" MemoryPool ${memNotifInfo.getPoolName()} -- Used =  ${memNotifInfo.getUsage().getUsed} Committed = ${memNotifInfo.getUsage.getCommitted}  Max = ${memNotifInfo.getUsage.getMax()} ")
             val pill = handback.asInstanceOf[PoisonPill]
             notification.getType() match {
                case  MemoryNotificationInfo.MEMORY_THRESHOLD_EXCEEDED => {
                  handback match {
                    case pp : PoisonPill => {

                       val before = memNotifInfo.getUsage.getUsed
                       pp.garbageCollect()
                       val after = pp.memPoolBean.getCollectionUsage().getUsed

                       val reclaimedPct =  (after - before)/memNotifInfo.getUsage.getMax 
                       if( reclaimedPct < MIN_RECLAIM_PCT ) {
                          info(s" Only Reclaimed  $reclaimedPct % of max available; Going to Die !!! ")
                          pp.dumpHeap
                          pp.die
                       } else {
                          info(s" Was able to reclaim $reclaimedPct bytes which is enough for now !!!!" )
                       }
                 }
               }
              }
              case MemoryNotificationInfo.MEMORY_COLLECTION_THRESHOLD_EXCEEDED => {
                  info(s" Received Collection Threshold Exceeded notification ")
              }
           }
       } else {
         info(s" Ignoring threshold notification for pool ${memNotifInfo.getPoolName()} ")
       }
       }
     }
     
    def garbageCollect() : Unit  = {
       info(s" Performing Garbage Collection !!!!")
       val memxBean = ManagementFactory.getMemoryMXBean
       memxBean.setVerbose(true)
       memxBean.gc
    }
    
    /**
     *  Take a Heap Dump !!!!
     *  ( Don't take one of mine !!!!)
     */
    def dumpHeap() = {
      info(s" Performing heapDump before dieing   ")
      val procId = ManagementFactory.getRuntimeMXBean().getName()
      val hprofFile = System.getProperty("user.dir") + "/java_" + procId + ".hprof"
      hotspotMBean.dumpHeap( hprofFile, true )
    }
     
    /**
     *  Since we can't recover, simply die, 
     *  and let monit restart the process
     */
    def die() = {
         info(" Unable to acquire more space ; Ready to die !!!!")
         warn(" Unable to acquire more space ; Ready to die !!!!")
         error(" Unable to acquire more space ; Ready to die !!!!")
         System.exit(1) 
     }
     
     val install = {
         
         val memxBean = ManagementFactory.getMemoryMXBean
          info( s"  MemBean is ${memxBean.getObjectName()}")
           memxBean match {
             case emitter : NotificationEmitter => {
                val maxPermGen : Long = memPoolBean.getUsage().getMax
                val threshold : Long = (thresholdPct*maxPermGen).toLong
                info(s" Adding PoisonPill if ${memPoolBean.getName()} pool usage exceeds $threshold or $thresholdPct % of $maxPermGen ")
                emitter.addNotificationListener(notificationListener, null, (this)) 
             }
             case _ => { error(s" MemBean $memxBean does not support notifications.") }
         }
     }

     // get the hotspot diagnostic MBean from the
    // platform MBean server
    def  hotspotMBean() : HotSpotDiagnosticMXBean = {
            val server = ManagementFactory.getPlatformMBeanServer();
             ManagementFactory.newPlatformMXBeanProxy(server,
                HOTSPOT_BEAN_NAME, classOf[HotSpotDiagnosticMXBean]);
    } 
}


object PoisonPill {
 
  
    def apply() : PoisonPill = {
      new PoisonPill(0.97, "CMS Perm Gen")
    }
  
  
}