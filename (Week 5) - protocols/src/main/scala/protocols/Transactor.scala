package protocols

import akka.actor.typed._
import akka.actor.typed.receptionist.Receptionist.Find
import akka.actor.typed.scaladsl._

import scala.concurrent.duration._

object Transactor {

  sealed trait PrivateCommand[T] extends Product with Serializable
  final case class Committed[T](session: ActorRef[Session[T]], value: T) extends PrivateCommand[T]
  final case class RolledBack[T](session: ActorRef[Session[T]]) extends PrivateCommand[T]

  sealed trait Command[T] extends PrivateCommand[T]
  final case class Begin[T](replyTo: ActorRef[ActorRef[Session[T]]]) extends Command[T]

  sealed trait Session[T] extends Product with Serializable
  final case class Extract[T, U](f: T => U, replyTo: ActorRef[U]) extends Session[T]
  final case class Modify[T, U](f: T => T, id: Long, reply: U, replyTo: ActorRef[U]) extends Session[T]
  final case class Commit[T, U](reply: U, replyTo: ActorRef[U]) extends Session[T]
  final case class Rollback[T]() extends Session[T]

  /**
    * @return A behavior that accepts public [[Command]] messages. The behavior
    *         should be wrapped in a [[SelectiveReceive]] decorator (with a capacity
    *         of 30 messages) so that beginning new sessions while there is already
    *         a currently running session is deferred to the point where the current
    *         session is terminated.
    * @param value Initial value of the transactor
    * @param sessionTimeout Delay before rolling back the pending modifications and
    *                       terminating the session
    */
  def apply[T](value: T, sessionTimeout: FiniteDuration): Behavior[Command[T]] =
      SelectiveReceive(30, idle(value, sessionTimeout).asInstanceOf[Behavior[Command[T]]])

  /**
    * @return A behavior that defines how to react to any [[PrivateCommand]] when the transactor
    *         has no currently running session.
    *         [[Committed]] and [[RolledBack]] messages should be ignored, and a [[Begin]] message
    *         should create a new session.
    *
    * @param value Value of the transactor
    * @param sessionTimeout Delay before rolling back the pending modifications and
    *                       terminating the session
    *
    * Note: To implement the timeout you have to use `ctx.scheduleOnce` instead of `Behaviors.withTimers`, due
    *       to a current limitation of Akka: https://github.com/akka/akka/issues/24686
    *
    * Hints:
    *   - When a [[Begin]] message is received, an anonymous child actor handling the session should be spawned,
    *   - In case the child actor is terminated, the session should be rolled back,
    *   - When `sessionTimeout` expires, the session should be rolled back,
    *   - After a session is started, the next behavior should be [[inSession]],
    *   - Messages other than [[Begin]] should not change the behavior.
    */
  private def idle[T](value: T, sessionTimeout: FiniteDuration): Behavior[PrivateCommand[T]] =
    Behaviors.receive {
      case (ctx, Begin(requester)) =>

        val done: Set[Long] = Set.empty
        val childSessionHandler = ctx.spawnAnonymous(sessionHandler(value, ctx.self, done))
        requester ! childSessionHandler
        ctx.watchWith(childSessionHandler, RolledBack(childSessionHandler))
        inSession(value, sessionTimeout, childSessionHandler)
      case (_, RolledBack(childSessionRef)) =>
        childSessionRef ! Rollback()
        Behaviors.same
      case (_, Committed(_, _)) =>
        Behaviors.same
    }


  /**
    * @return A behavior that defines how to react to [[PrivateCommand]] messages when the transactor has
    *         a running session.
    *         [[Committed]] and [[RolledBack]] messages should commit and rollback the session, respectively.
    *         [[Begin]] messages should be unhandled (they will be handled by the [[SelectiveReceive]] decorator).
    *
    * @param rollbackValue Value to rollback to
    * @param sessionTimeout Timeout to use for the next session
    * @param sessionRef Reference to the child [[Session]] actor
    */
  private def inSession[T](rollbackValue: T, sessionTimeout: FiniteDuration, sessionRef: ActorRef[Session[T]]): Behavior[PrivateCommand[T]] =
    Behaviors.setup { ctx =>
      ctx.setReceiveTimeout(sessionTimeout, RolledBack(sessionRef))
      sessionHandlerMessageReceiver(rollbackValue, sessionTimeout, sessionRef)
    }

  private def sessionHandlerMessageReceiver[T](rollbackValue: T, sessionTimeout: FiniteDuration, sessionRef: ActorRef[Session[T]]): Behavior[PrivateCommand[T]] =
    Behaviors.receivePartial {
      case (_, Committed(_, updatedValue)) =>
        idle(updatedValue, sessionTimeout)
      case (ctx, RolledBack(childSessionRef)) =>
        ctx stop childSessionRef
        idle(rollbackValue, sessionTimeout)
    }


  /**
    * @return A behavior handling [[Session]] messages. See in the instructions
    *         the precise semantics that each message should have.
    *
    * @param currentValue The session’s current value
    * @param commit Parent actor reference, to send the [[Committed]] message to
    * @param done Set of already applied [[Modify]] messages
    */
  private def sessionHandler[T](currentValue: T, commit: ActorRef[Committed[T]], done: Set[Long]): Behavior[Session[T]] = Behaviors.receive {
    /*
    Extract: apply the given projector function to the current
    Transactor value (possibly modified by the current session)
    and return the result to the given ActorRef; if the projector
    function throws an exception the session is terminated
    and all its modifications are rolled back
    */
    case (_, Extract(fn, replyTo)) =>
      replyTo ! fn(currentValue)
      Behaviors.same

    /*
    Modify: calculate a new value for the Transactor by applying
    the given function to the current value (possibly modified by
    the current session already) and return the given reply value
    to the given replyTo ActorRef; if the function throws an exception
    the session is terminated and all its modifications are rolled back;
    the id argument serves as a deduplication identifier: sending the a
    Modify command with an id previously used within the current session
    will not apply the modification function but send the reply immediately
    */
    case (_, Modify(fn, id, reply, replyTo)) =>
      if(done contains id) {
        replyTo ! reply
        Behaviors.same
      } else {
        val updatedValue = fn(currentValue)
        replyTo ! reply
        val updatedDone = done + id
        sessionHandler(updatedValue, commit, updatedDone)
      }

    /*
    Commit: terminate the current session, committing the performed
    modifications and thus making the modified value available to
    the next session; the given reply is sent to the given ActorRef as confirmation
     */
    case (ctx, Commit(reply, replyTo)) =>
      replyTo ! reply
      commit ! Committed(ctx.self, currentValue)
      Behaviors.stopped

    /*
    Rollback: terminate the current session rolling back all modifications,
    i.e. the next session will see the same value that this session saw when it started
     */
    case (_, Rollback()) =>
      Behaviors.stopped
  }


}
