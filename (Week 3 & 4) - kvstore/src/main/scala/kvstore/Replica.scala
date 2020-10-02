package kvstore

import akka.actor.{ OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, ActorRef, Actor }
import kvstore.Arbiter._
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout
import scala.language.postfixOps

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case class OperationTimeout(id: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  // the persistent message we send, but not yet persist acknowledge
  var persistenceSend = Map.empty[Long, (ActorRef, Option[Persist], Set[ActorRef])]

  var expectedSequence: Long = 0

  val persistence = context.actorOf(persistenceProps)

  override def preStart(): Unit = {
    arbiter ! Join
    context.system.scheduler.schedule(100 millis, 100 millis)(resendPersist())
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key : String, value : String, id : Long) =>
      kv += key -> value
      triggerPersistPrimary(key, Some(value), id)

    case Remove(key : String, id : Long) =>
      kv -= key
      triggerPersistPrimary(key, None, id)

    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)

    case Persisted(_, id) =>
      persistenceSend.get(id).foreach {
        case (ref, _, set) =>
          persistenceSend = persistenceSend.updated(id, (ref, None, set))
      }
      checkFinished(id)

    case Replicas(replicas) =>
      val removed = (secondaries.keySet -- replicas).filterNot(_ == self)
      val added = (replicas -- secondaries.keySet).filterNot(_ == self)

      removed.foreach { ref =>
        val replicatorRef = secondaries(ref)
        replicatorRef ! PoisonPill

        secondaries -= ref
        replicators -= ref

        persistenceSend.foreach {
          case (id, (r, msg, refs)) =>
            persistenceSend = persistenceSend.updated(id, (r, msg, refs - replicatorRef))
            checkFinished(id)
        }
      }

      added.foreach { ref =>
        val replicator = context.actorOf(Replicator.props(ref))
        secondaries += ref -> replicator
        replicators += replicator

        replicators.zipWithIndex.foreach {
          case (replicatorRef, index) => kv.foreach {
            case (key, value) => replicatorRef ! Replicate(key, Option(value), index)
          }
        }
      }

    case Replicated(key, id) =>
      persistenceSend.get(id).foreach {
        case(ref, message, set) =>
          persistenceSend = persistenceSend.updated(id, (ref, message, set - sender()))
      }
      checkFinished(id)

    case OperationTimeout(id) =>
      persistenceSend.get(id).foreach {
        case (ref, _, _) => ref ! OperationFailed(id)
      }
      persistenceSend -= id
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Snapshot(key, valueOption, seq) =>
      if (seq == expectedSequence){
        valueOption match {
          case Some(value) => kv += key -> value
          case None => kv -= key
        }
        triggerPersistence(key, valueOption, seq)
      } else if (seq < expectedSequence) {
        sender ! SnapshotAck(key, seq)
      }


    case Persisted(key, seq) =>
      persistenceSend.get(seq).foreach {
        case (r, _, _) =>
          r ! SnapshotAck(key, seq)
          expectedSequence += 1
      }
      persistenceSend -= seq
  }

  def triggerPersistPrimary(key: String, valueOption: Option[String], seq: Long) = {
    triggerPersistence(key, valueOption, seq)
    replicators.foreach(_ ! Replicate(key, valueOption, seq))
    context.system.scheduler.scheduleOnce(1 second, self, OperationTimeout(seq))
  }

  def triggerPersistence(key: String, valueOption: Option[String], seq: Long) = {
    val persistMsg = Persist(key, valueOption, seq)
    persistenceSend += seq -> (sender(), Some(persistMsg), replicators)
    persistence ! persistMsg
  }

  def checkFinished(id: Long) = {
    persistenceSend.get(id).foreach {
      case (ref, None, s) if s.isEmpty =>
        ref ! OperationAck(id)
        persistenceSend -= id
      case _ =>
    }
  }

  def resendPersist() = persistenceSend.values.foreach {
    case (_, Some(m), _) => persistence ! m
    case _ =>
  }
}

