package io.wonder.soft.mbs.streams

import akka.actor.Actor
import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener

import scala.collection.mutable
import com.github.shyiko.mysql.binlog.event.{Event => BinLogEvent}
import com.typesafe.config.{Config, ConfigFactory}

// want to change to use mutable object..
object BinLogQue {
  val que = new mutable.Queue[BinLogEvent]()
}

class BinLogQueActor extends Actor {
  val config: Config = ConfigFactory.load()
  val dbDefault = config.getConfig("db.default")

  val dbHost = dbDefault.getString("host")
  val dbUser = dbDefault.getString("user")
  val dbPassword = dbDefault.getString("password")
  val dbPort = dbDefault.getInt("port")

  val client = new BinaryLogClient(dbHost, dbPort, dbUser, dbPassword)

  def receive = {
    case 'init =>
      client.registerEventListener(
        new EventListener {
          def onEvent(event: BinLogEvent): Unit = {
            if (event.getData != null) {
              BinLogQue.que.enqueue(event)
            }
          }
        }
      )
      client.connect()
  }
}
