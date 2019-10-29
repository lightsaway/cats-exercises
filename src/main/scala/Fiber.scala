import Scope.{CallBacks, log}

import scala.concurrent.duration._
import cats.{Parallel, Traverse}
import cats.effect._
import cats.effect.concurrent._
import cats.effect.implicits._
import cats.implicits._

trait Nursery[F[_]] {
  def startSoon[A](fa: F[A]): F[Fiber[F, A]]
}

object Nursery {
  // Implement this. You need `Concurrent` since you'd be using `.start` internally,
  // and you should NOT need anything else
  /*
Here’s how the Resource[F, Nursery[F]] should behave:

The resource is finalized only when all fibers produced by startSoon are finished or explicitly cancelled.
If the use is cancelled or results in error, all fibers produced by startSoon should be cancelled.
startSoon should be atomic - there should be no potential for fiber to leak outside the control of the Nursery. It’s either started and will not leak, or not started, if use is cancelled.
If any fiber has errored, the error should be propagated. Existing fibers should be cancelled and no new ones should be accepted. (optional - hard, but needed for parity with Parallel)
   */
  def log[F[_]](s: String)(implicit F: Sync[F]) = F.delay(println(s))

  def apply[F[_]](implicit F: Concurrent[F]): Resource[F, Nursery[F]] =
    Resource
      .makeCase(Ref.of[F, List[Fiber[F, _]]](Nil)) { (cbs, status) => cbs.get.flatMap{
        // WIP this is just hacking to make test pass
        case a => a.foldLeft(().asRight[Throwable].pure[F]) {
          case (e, cb) => e.flatMap(err =>  (err, status) match {
            case ( _ , ExitCase.Canceled)  => cb.cancel.as(err)
            case ( _ , ExitCase.Error(ex)) => cb.cancel.as(ex.asLeft)
            case (Left(_),  ExitCase.Completed) => cb.cancel.as(err)
            case (Right(_), ExitCase.Completed) => cb.cancel
              .attempt.flatMap(either =>
              e.map(errorAcc => errorAcc.flatMap(_ => either.map(_ => ()))))
          } )
        }
          .rethrow
      } <* log[F](s"status of  ${status}")}
      .map { fibers =>
        new Nursery[F] {
          override def startSoon[A](fa: F[A]): F[Fiber[F, A]] = {
            fa.start.flatMap(
              f => fibers.update(s => s :+ f).as(f)
//                .guaranteeCase{
//                case ExitCase.Canceled => fibers.update(f => f.filter(_!=f))
//                case _ => F.unit
//              }
                          )
          }
        }
      }

}

object FiberEx extends IOApp {
  def concSequence[F[_]: Concurrent, G[_]: Traverse, A](gfa: G[F[A]]): F[G[A]] =
    Nursery[F].use { n =>
      gfa.traverse(n.startSoon).flatMap(_.traverse(_.join))
    }

  override def run(args: List[String]): IO[ExitCode] =
    happyPath[IO] >>
      IO(println("happyPath completed!")) >>
      nestedUsage[IO] >>
      IO(println("nestedUsage completed!")) >>
      manualCancelWorks[IO] >>
      IO(println("manualCancelWorks completed!")) >>
      errorsCancelOthers[IO] >>
      IO(println("errorsCancelOthers completed!")) >>
      cancellationIsPropagated[IO] >>
      IO(println("cancellationIsPropagated completed!")) >>
      parSequenceParity[IO] >>
      IO(println("Run completed!")) >>
      IO.pure(ExitCode.Success)

  def happyPath[F[_]: Concurrent] = test[F] { n =>
    for {
      d1 <- Deferred[F, Unit]
      d2 <- Deferred[F, Unit]
      _  <- n.startSoon(d1.get)
      _  <- n.startSoon(d2.get)
      _  <- n.startSoon(d1.complete(()))
      _  <- n.startSoon(d2.complete(()))
      _  <- n.startSoon(().pure[F])
      _  <- n.startSoon(().pure[F])
      _  <- n.startSoon(().pure[F])
      _  <- n.startSoon(().pure[F])
    } yield ()
  }

  def nestedUsage[F[_]: Concurrent] = test[F] { n =>
    n.startSoon {
      for {
        d1 <- Deferred[F, Unit]
        d2 <- Deferred[F, Unit]
        d3 <- Deferred[F, Unit]
        _  <- n.startSoon(d1.get >> n.startSoon(d2.complete(())))
        _ <- n.startSoon {
          n.startSoon(d1.complete(())) >>
            n.startSoon(d2.get) >>
            n.startSoon(d3.complete(()))
        }
        _ <- d3.get
      } yield ()
    }.void
  }

  def manualCancelWorks[F[_]: Concurrent: Timer] = test[F] { n =>
    for {
      d <- Deferred[F, Unit]
      f <- n.startSoon(Timer[F].sleep(1.second).guaranteeCase {
        case ExitCase.Canceled => d.complete(())
        case _                 => new Exception().raiseError[F, Unit]
      })
      _ <- f.cancel
      _ <- d.get
    } yield ()
  }

  def errorsCancelOthers[F[_]: Concurrent] = test[F] { n =>
    for {
      d <- Deferred[F, Unit]
      _ <- n.startSoon(Concurrent[F].never[Unit].guaranteeCase {
        case ExitCase.Canceled => d.complete(())
        case _                 => new Exception("Expected cancellation").raiseError[F, Unit]
      })
      _ <- n.startSoon(ExpectedException.raiseError[F, Unit])
    } yield ()
  }

  def cancellationIsPropagated[F[_]: Concurrent] = {
    for {
      d1 <- Deferred[F, Unit]
      d2 <- Deferred[F, Unit]
      d3 <- Deferred[F, Unit]
      f <- test[F] { n =>
        n.startSoon(d1.complete(()) >> Concurrent[F].never[Unit].onCancel(d3.complete(()))) >>
          d2.get
      }.start
      _ <- d1.get
      _ <- f.cancel
      _ <- d2.complete(())
      _ <- d3.get
    } yield ()
  }

  def parSequenceParity[F[_]: Concurrent: Timer: Parallel] =
    Ref[F].of(Map.empty[String, Int]).flatMap { log =>
      val crash = (Timer[F].sleep(350.millis) >> new Exception().raiseError[F, Unit])
      val jobs =
        (crash :: (100 to 500 by 100).map(_.millis).map(Timer[F].sleep).toList).map { job =>
          job.guaranteeCase(ec =>
            log.update { map =>
              map |+| Map(ec.getClass.getName -> 1)
          })
        }

      for {
        _       <- jobs.parSequence.attempt
        parLog  <- log.getAndSet(Map())
        _       <- concSequence(jobs).attempt
        concLog <- log.getAndSet(Map())
        _ <- Sync[F].delay {
          require(parLog == concLog)
        }
      } yield ()
    }

  def trackedNursery[F[_]: Concurrent](ref: Ref[F, Int]): Resource[F, Nursery[F]] =
    Nursery[F].map { n =>
      new Nursery[F] {
        override def startSoon[A](fa: F[A]): F[Fiber[F, A]] =
          n.startSoon(ref.update(_ + 1).bracket(_ => fa)(_ => ref.update(_ - 1)))
      }
    }

  object ExpectedException extends Exception
  def test[F[_]: Concurrent](run: Nursery[F] => F[Unit]): F[Unit] =
    Ref[F]
      .of(0)
      .flatMap { active =>
        trackedNursery(active).use(run).attempt <* active.get.flatMap {
          case 0 => ().pure[F]
          case other =>
            new Exception(s"Active fiber count is $other, expected 0").raiseError[F, Unit]
        }
      }
      .rethrow
      .recover { case ExpectedException => () }
}
