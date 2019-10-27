import cats.effect.IOApp

object WorkerPool extends IOApp {
  import cats.effect.concurrent.MVar
  import scala.util.Random
  import scala.concurrent.duration._
  import cats.implicits._
  import cats.effect.{ExitCode, IO, Timer}
  import cats.effect.concurrent.Ref
  import cats.effect.syntax.concurrent._
  import cats.effect.syntax.bracket._
  import cats.effect.Concurrent

  type Worker[F[_], A, B] = A => F[B]

  def log[F[_]: Concurrent](s: String) = Concurrent[F].delay(println(s))

  def mkWorker[F[_]: Concurrent](id: Int)(implicit timer: Timer[F]): F[Worker[F, Int, Int]] =
    Ref[F].of(0).map { counter =>
      def simulateWork: F[Unit] =
        Concurrent[F].delay(50 + Random.nextInt(450)).map(_.millis).flatMap(timer.sleep)

      def report: F[Unit] =
        counter.get.flatMap(i => log[F](s"Total processed by $id: $i"))

      x =>
        simulateWork >>
          counter.update(_ + 1) >>
          report >>
          Concurrent[F].pure(x + 1)
    }

  trait WorkerPool[F[_], A, B] {
    def exec(a: A): F[B]

    def removeAllWorkers: F[Unit]

    def addWorker(w: Worker[F, A, B]): F[Unit]
  }

  object WorkerPool {
    def of[F[_]: Concurrent, A, B](fs: List[Worker[F, A, B]]): F[WorkerPool[F, A, B]] =
      for {
        queue    <- MVar[F].empty[Worker[F, A, B]]
        _        <- fs.traverse_(queue.put).start.void
        contract <- Ref.of[F, Set[Worker[F, A, B]]](fs.toSet)
        pool = new WorkerPool[F, A, B] {

          override def exec(a: A): F[B] =
            for {
              w       <- queue.take
              isFired <- contract.get.map(!_.contains(w))
              b       <- w(a).guarantee(if (isFired) Concurrent[F].unit else queue.put(w).start.void)
            } yield b

          override def removeAllWorkers: F[Unit] =
            contract.set(Set.empty[Worker[F, A, B]]).void *>
              log[F]("all workers have been removed")

          override def addWorker(w: Worker[F, A, B]): F[Unit] =
            contract.modify(workers => (workers + w, w)) *>
              queue.put(w).start.void *>
              log[F]("new worker added to pool")
        }
      } yield pool
  }

  // Sample test pool to play with in IOApp
  val testPool: IO[WorkerPool[IO, Int, Int]] =
    List
      .range(0, 10)
      .traverse(mkWorker[IO])
      .flatMap(WorkerPool.of[IO, Int, Int])

  def run(args: List[String]) =
    for {
      pool <- testPool
      _    <- pool.exec(42).start
      _    <- pool.removeAllWorkers
      _    <- pool.exec(10).replicateA(10)
    } yield ExitCode.Success
}
