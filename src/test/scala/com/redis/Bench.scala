package com.redis

object Bench {
  object Values {
    val values = Iterator.continually("foo")
  }

  def listFn(count: Int, key: String)(implicit clients: RedisClientPool): Unit = {
    import Values._
    clients.withClient {
      client => {
        (1 to count) foreach (i => client.rpush(key, values.next))
        assert(client.llen(key) == Some(count))
        (1 to count) foreach (i => client.lpop(key))
        assert(client.llen(key) == Some(0))
      }
    }
  }

  def incrFn(count: Int, key: String)(implicit clients: RedisClientPool): Unit = {
    clients.withClient {
      client => {
        client.set(key, 0L)
        (1 to count) foreach (i => client.incr(key))
      }
    }
  }

  def listLoad(opsPerClient: Int)(implicit clients: RedisClientPool) = load(opsPerClient, listFn)
  def incrLoad(opsPerClient: Int)(implicit clients: RedisClientPool) = load(opsPerClient, incrFn)

  def load(opsPerClient: Int, fn: (Int, String) => Unit)(implicit clients: RedisClientPool): (Double, Double, Seq[_]) = {
    val start = System.nanoTime
    val tasks = (1 to 100) map (i => actors.Futures.future { fn(opsPerClient, "k" + i.toString) })
    val results = tasks map (future => future.apply())
    val elapsedSeconds = (System.nanoTime - start)/1000000000.0 
    val opsPerSec = (opsPerClient * 100 * 2) / elapsedSeconds
    (elapsedSeconds, opsPerSec, results)
  }
}
