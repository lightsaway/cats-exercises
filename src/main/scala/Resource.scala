import scala.concurrent.duration._

import cats.effect._
import cats.effect.concurrent._
import cats.effect.implicits._
import cats.implicits._

sealed trait Scope[F[_]] {
  def open[A](ra: Resource[F, A]): F[A]
}
// http
// db

object Scope {
  type CallBacks[F[_]] = List[(String, F[Unit])]
  def log[F[_]](s: String)(implicit F: Sync[F]) = F.delay(println(s))
  def apply[F[_]: Sync]: Resource[F, Scope[F]] =
    Resource
      .make(Ref.of[F, CallBacks[F]](Nil)) { ref =>
        ref.get.flatMap { callbacks =>
          callbacks
            .foldLeft(().asRight[Throwable].pure[F]) {
              case (errorChannel, (name, cb)) =>
                log[F](s"releasing resource ${name}") *>
                  cb.attempt.flatMap(either => errorChannel.as(either))
            }
            .rethrow
        }
      }
      .map { callbacks =>
        new Scope[F] {
          def open[A](ra: Resource[F, A]): F[A] =
            ra.allocated.bracket { case (a, _) => a.pure[F] } {
              case (a, cb) =>
                callbacks.update(_ :+ (a.toString -> cb)) <*
                  log[F](s"registering callback for ${a.toString}")
            }
        }
      }
}

object ResourceEx extends IOApp {
  case class TestResource(idx: Int)

  override def run(args: List[String]): IO[ExitCode] = {
    happyPath[IO] >>
      atomicity[IO] >>
      cancelability[IO] >>
      errorInRelease[IO] >>
      errorInAcquire[IO] >>
      IO(println("Run completed")) >>
      IO.pure(ExitCode.Success)
  }

  case class Allocs[F[_]](
      normal: Resource[F, TestResource],
      slowAcquisition: Resource[F, TestResource],
      crashOpen: Resource[F, TestResource],
      crashClose: Resource[F, TestResource],
  )

  def happyPath[F[_]: Concurrent: Timer] =
    test[F] { (allocs, scope, _) =>
      for {
        r1 <- scope.open(allocs.normal)
        r2 <- scope.open(allocs.slowAcquisition)
        r3 <- scope.open(allocs.normal)
      } yield ()
    } { (allocs, deallocs, ec) =>
      Sync[F].delay {
        require(allocs == Vector(1, 2, 3))
        require(allocs == deallocs.reverse)
        require(ec == ExitCase.Completed)
      }
    }

  def atomicity[F[_]: Concurrent: Timer] =
    test[F] { (allocs, scope, cancelMe) =>
      for {
        r1   <- scope.open(allocs.normal)
        lock <- Deferred[F, Unit]
        _    <- (lock.get >> cancelMe).start
        r2   <- scope.open(Resource.liftF(lock.complete(())) >> allocs.slowAcquisition)
        _    <- Timer[F].sleep(1.second)
        r3   <- scope.open(allocs.normal)
      } yield ()
    } { (allocs, deallocs, ec) =>
      Sync[F].delay {
        require(allocs == Vector(1, 2))
        require(deallocs == Vector(2, 1))
        require(ec == ExitCase.Canceled)
      }
    }

  def cancelability[F[_]: Concurrent: Timer] =
    test[F] { (allocs, scope, cancelMe) =>
      for {
        r1 <- scope.open(allocs.normal)
        r2 <- scope.open(allocs.slowAcquisition)
        _  <- cancelMe
        _  <- Timer[F].sleep(1.second)
        r3 <- scope.open(allocs.normal)
      } yield ()
    } { (allocs, deallocs, ec) =>
      Sync[F].delay {
        require(allocs == Vector(1, 2))
        require(deallocs == Vector(2, 1))
        require(ec == ExitCase.Canceled)
      }
    }

  def errorInRelease[F[_]: Concurrent: Timer] =
    test[F] { (allocs, scope, _) =>
      for {
        r1 <- scope.open(allocs.normal)
        r2 <- scope.open(allocs.crashClose)
        r3 <- scope.open(allocs.normal)
      } yield ()
    } { (allocs, deallocs, ec) =>
      Sync[F].delay {
        require(allocs == Vector(1, 2, 3))
        require(deallocs == Vector(3, 1))
        require(ec match {
          case ExitCase.Error(_) => true
          case _                 => false
        })
      }
    }

  def errorInAcquire[F[_]: Concurrent: Timer] =
    test[F] { (allocs, scope, _) =>
      for {
        r1 <- scope.open(allocs.normal)
        r2 <- scope.open(allocs.crashOpen)
        r3 <- scope.open(allocs.normal)
      } yield ()
    } { (allocs, deallocs, ec) =>
      Sync[F].delay {
        require(allocs == Vector(1))
        require(deallocs == Vector(1))
        require(ec match {
          case ExitCase.Error(_) => true
          case _                 => false
        })
      }
    }

  def test[F[_]: Concurrent: Timer](run: (Allocs[F], Scope[F], F[Unit]) => F[Unit])(
      check: (Vector[Int], Vector[Int], ExitCase[Throwable]) => F[Unit]
  ): F[Unit] =
    for {
      idx        <- Ref[F].of(1)
      allocLog   <- Ref[F].of(Vector.empty[Int])
      deallocLog <- Ref[F].of(Vector.empty[Int])
      open = idx
        .modify(i => (i + 1, i))
        .flatTap(i => allocLog.update(_ :+ i))
        .map(TestResource)
      close = (r: TestResource) => deallocLog.update(_ :+ r.idx)
      cancel <- Deferred[F, Unit]
      slow = Timer[F].sleep(1.second)
      allocs = Allocs[F](
        Resource.make(open)(close),
        Resource.make(slow >> open)(close),
        Resource(Sync[F].raiseError(new Exception)),
        Resource.make(open)(_ => Sync[F].raiseError(new Exception))
      )
      finish <- Deferred[F, Either[Throwable, Unit]]
      scope = for {
        _ <- Resource.makeCase(().pure[F])((_, ec) => {
          (allocLog.get, deallocLog.get)
            .mapN(check(_, _, ec))
            .flatten
            .attempt
            .flatMap(finish.complete)
        })
        s <- Scope[F]
      } yield s
      _ <- scope
        .use(run(allocs, _, cancel.complete(())))
        .race(cancel.get)
        .attempt
      _ <- finish.get.rethrow
    } yield ()
}
