package concurrent
import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent._
import scala.concurrent.duration._
import scala.collection.mutable.Queue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global // eww

object Concurrent {
  type Result = String
  type Search = Function1[String, Result]

  val system = ActorSystem("concurrent")
  implicit val timeout = Timeout(FiniteDuration(10, TimeUnit.SECONDS))

  val Web1 = fakeSearch("web1")
  val Web2 = fakeSearch("web2")
  val Image1 = fakeSearch("image1")
  val Image2 = fakeSearch("image2")
  val Video1 = fakeSearch("video1")
  val Video2 = fakeSearch("video2")

  case object GetMessage
  case object NoMessages
  case object AllMessagesSent
  case object SearchTimeout

  def Google(query: String): List[Result] = {
    val c = makeCollector(3)

    First(query, Web1, Web2).foreach(c ! _)
    First(query, Image1, Image2).foreach(c ! _)
    First(query, Video1, Video2).foreach(c ! _)
    makeTimeout(Duration("500 millis")).foreach(c ! _)

    val results = List.newBuilder[Result]

    // Note: this doesn't actually handle the case where all the generators are done.
    def waitUntilTimeout(): Unit = {
      Await.result(c ? GetMessage, Duration("10 seconds")) match {
        case res: Result =>
          println(s"got result $res")
          results += res

        case NoMessages => ()

        case AllMessagesSent =>
          println("got all messages")
          return

        case SearchTimeout =>
          println("got search timeout")
          return

        case err =>
          println(s"uhhhh, something is broken: $err")
      }
      waitUntilTimeout()
    }
    waitUntilTimeout()

    return results.result
  }

  private[this] def First(query: String, searches: Search*): Future[Result] =
    Future.firstCompletedOf(searches.map(f => Future(f(query))).toList)

  private[this] def makeCollector(expected: Int): ActorRef = {
    system.actorOf(Props(new Actor {
      val queue = new Queue[Result]
      val count = new AtomicInteger(0)
      def receive = {
        case result: Result =>
          queue.enqueue(result)
          count.incrementAndGet()

        case GetMessage if count.get == expected =>
          val listener = sender
          listener ! AllMessagesSent

        case GetMessage if queue.isEmpty =>
          val listener = sender
          listener ! NoMessages

        case GetMessage =>
          val listener = sender
          listener ! queue.dequeue

        case SearchTimeout =>
          val listener = sender
          listener ! SearchTimeout

        case unhandled =>
          println(s"unhandled message of ${unhandled}")
      }
    }))
  }

  private[this] def makeTimeout(timeout: Duration) =
    Future {
      Thread.sleep(timeout.toMillis)
      SearchTimeout
    }

  private[this] def fakeSearch(kind: String): Search = { (query: String) =>
    Thread.sleep(scala.util.Random.nextInt(100))
    s"$kind result for $query"
  }

  def main(args: Array[String]): Unit = {
    val start = (new java.util.Date).getTime
    val results = Google("golang")
    val end = (new java.util.Date).getTime
    println(results)
    println(s"${end - start} ms")
  }
}
