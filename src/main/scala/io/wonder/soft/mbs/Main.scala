package io.wonder.soft.mbs

import akka.actor.{ActorSystem, Props}
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.{Flow, Sink, Source}
import io.wonder.soft.mbs.streams._

import scala.concurrent.ExecutionContextExecutor

object Main {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem  = ActorSystem("BinLogStream")
    implicit val executor: ExecutionContextExecutor = system.dispatcher
    implicit val materializer: Materializer = ActorMaterializer()

    val binLogQueActor = system.actorOf(Props[BinLogQueActor])
    binLogQueActor ! 'init

    val binLogSource = new BinLogSource
    val binLogFlow = new BinLogFlow
    val binLogSink = new BinLogSink
    val runnableGraph = Source.fromGraph(binLogSource).via(Flow.fromGraph(binLogFlow)).to(Sink.fromGraph(binLogSink))
    runnableGraph.run()
  }
}
